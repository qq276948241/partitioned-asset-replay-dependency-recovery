"""Operator entry points that run the whole drill and write checkable artifacts."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import dagster as dg

from . import config, lake, planner, runner, sample_data, verify
from .depmap import write_dependency_map
from .provenance import read_json, stable_id, utc_now, write_json_atomic


def _prepare() -> None:
    for directory in config.all_directories():
        directory.mkdir(parents=True, exist_ok=True)
    dagster_home = config.dagster_home()
    if not (dagster_home / "dagster.yaml").exists():
        write_json_atomic(dagster_home / "dagster.yaml", {})
    # Bind this process (and every instance it creates) to the workspace's home.
    # A stale DAGSTER_HOME inherited from the shell would silently split runs and
    # result files into different stores, so it is overridden rather than defaulted.
    os.environ["DAGSTER_HOME"] = str(dagster_home)


def _report(name: str, payload: dict) -> str:
    path = config.reports_dir() / f"{name}.json"
    write_json_atomic(path, payload)
    return str(path)


def _print(message: str) -> None:
    print(message, flush=True)


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def cmd_generate(_args: argparse.Namespace) -> int:
    _prepare()
    result = sample_data.generate_initial(
        config.DEFAULT_PARTITIONS, config.INITIAL_MISSING_ORDERS, config.INITIAL_MISSING_FX
    )
    inventory = {
        "schema": "sample_generation/1",
        "generated_at": utc_now(),
        "partitions": config.DEFAULT_PARTITIONS,
        "written": result["written"],
        "skipped": result["skipped"],
        "withheld_gaps": result["gaps"],
        "source_revisions": sample_data.source_inventory(config.DEFAULT_PARTITIONS),
    }
    path = _report("01_sample_generation", inventory)
    _print(
        f"generated {len(result['written'])} source revisions; withheld {len(result['gaps'])} partitions"
    )
    _print(f"  report: {path}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    _prepare()
    reason = args.reason
    plan = planner.build_plan(reason)
    path = planner.write_plan(plan, args.name)
    _report(args.name, plan)
    _print(
        f"plan {plan['plan_id']}: {plan['totals']['computable']} computable / "
        f"{plan['totals']['not_computable']} not computable (reason={reason})"
    )
    _print(f"  artifact: {path}")
    return 0


def run_plan(plan: dict, request_id: str) -> dict:
    result = runner.execute_plan(plan, request_id)
    return result


def cmd_run(args: argparse.Namespace) -> int:
    _prepare()
    plan = planner.build_plan(args.reason)
    planner.write_plan(plan, args.name)

    # A repeated request is naturally idempotent: the planner marks committed,
    # up-to-date partitions ``already_current`` so they never enter ``computable``.
    request_id = args.request or stable_id("req", {"reason": args.reason, "plan": plan["plan_id"]})
    result = run_plan(plan, request_id)
    runner.record_outcomes(result["outcomes"], plan, request_id)
    path = _report(args.name, result)
    _print(
        f"run {request_id}: attempted={result['attempted']} succeeded={result['succeeded']} "
        f"failed={result['failed']} accepted={len(result['accepted_results'])}"
    )
    _print(f"  artifact: {path}")
    return 0 if result["failed"] == 0 else 1


def cmd_fail(args: argparse.Namespace) -> int:
    """Launch one work item with a fault and capture the partial material it leaves."""
    _prepare()
    lake.quarantine_partial_attempts()
    os.environ[config.ENV_FAULT_STAGE] = args.stage
    os.environ[config.ENV_FAULT_ACTION] = args.action
    os.environ[config.ENV_FAULT_ASSET] = args.asset
    os.environ[config.ENV_FAULT_PARTITION] = args.partition
    os.environ[config.ENV_FAULT_TOKEN] = args.token

    outcome = runner.launch_work_item(args.asset, args.partition, "fault_drill", args.request)
    record = {
        "schema": "fault_injection/1",
        "injected_at": utc_now(),
        "stage": args.stage,
        "action": args.action,
        "asset": args.asset,
        "partition": args.partition,
        "request_id": args.request,
        "outcome": outcome,
        "staging_state": _staging_snapshot(),
    }
    path = _report(f"05_fault_{args.stage}", record)
    _print(
        f"fault {args.stage}/{args.action} on {args.asset}/{args.partition}: "
        f"run_status={outcome['run_status']} materialized={outcome['materialized']}"
    )
    _print(f"  artifact: {path}")
    return 0


def _staging_snapshot() -> list[dict]:
    return lake.scan_attempts()


def cmd_recover(args: argparse.Namespace) -> int:
    """Reconcile partial material, then recompute only what is genuinely affected."""
    _prepare()
    quarantined = lake.quarantine_partial_attempts()
    plan = planner.build_plan(f"recovery:{args.scope}")
    planner.write_plan(plan, args.name)

    request_id = args.request or stable_id("req", {"recovery": args.scope, "plan": plan["plan_id"]})
    result = run_plan(plan, request_id)
    runner.record_outcomes(result["outcomes"], plan, request_id)
    result["quarantined_attempts"] = quarantined
    path = _report(args.name, result)
    _print(
        f"recovery {request_id}: quarantined={len(quarantined)} attempted={result['attempted']} "
        f"succeeded={result['succeeded']} failed={result['failed']}"
    )
    _print(f"  artifact: {path}")
    return 0 if result["failed"] == 0 else 1


def cmd_restore_gaps(_args: argparse.Namespace) -> int:
    _prepare()
    result = sample_data.restore_gaps(
        config.DEFAULT_PARTITIONS, config.INITIAL_MISSING_ORDERS, config.INITIAL_MISSING_FX
    )
    payload = {
        "schema": "gap_restoration/1",
        "restored_at": utc_now(),
        "written": result["written"],
        "skipped": result["skipped"],
        "source_revisions": sample_data.source_inventory(config.DEFAULT_PARTITIONS),
    }
    path = _report("06_gap_restoration", payload)
    _print(
        f"restored {len(result['written'])} late source revisions; skipped {len(result['skipped'])}"
    )
    _print(f"  artifact: {path}")
    return 0


def cmd_correct(args: argparse.Namespace) -> int:
    """Append a source correction and record the scope it should affect."""
    _prepare()
    changed = sample_data.correct_partition(
        config.DEFAULT_PARTITIONS, args.asset, args.partition, args.salt
    )
    scope = planner.affected_scope(
        [{"asset": args.asset, "partition": args.partition}], config.DEFAULT_PARTITIONS
    )
    payload = {
        "schema": "correction/1",
        "corrected_at": utc_now(),
        "changed_source": changed,
        "affected_scope": scope,
    }
    path = _report("07_correction", payload)
    _print(
        f"corrected {args.asset}/{args.partition} (seq {changed['previous_seq']} -> {changed['seq']}); "
        f"{len(scope['affected'])} downstream partitions affected"
    )
    _print(f"  artifact: {path}")
    return 0


def cmd_repair_scope(args: argparse.Namespace) -> int:
    """Recompute exactly the affected scope after a correction."""
    _prepare()
    correction = read_json(config.reports_dir() / "07_correction.json")
    scope = correction["affected_scope"]
    plan = planner.scope_plan(f"correction:{args.scope}", scope, config.DEFAULT_PARTITIONS)
    planner.write_plan(plan, args.name)

    request_id = args.request or stable_id(
        "req", {"correction": args.scope, "plan": plan["plan_id"]}
    )
    result = run_plan(plan, request_id)
    runner.record_outcomes(result["outcomes"], plan, request_id)
    result["scope"] = scope
    path = _report(args.name, result)
    _print(
        f"scoped recovery {request_id}: attempted={result['attempted']} "
        f"succeeded={result['succeeded']} failed={result['failed']}"
    )
    _print(f"  artifact: {path}")
    return 0 if result["failed"] == 0 else 1


def cmd_cancel(args: argparse.Namespace) -> int:
    """Start a real Dagster run and cancel it from outside, recording the outcome."""
    _prepare()
    instance = dg.DagsterInstance.get()
    env = child_env()
    env[config.ENV_FAULT_STAGE] = "read"
    env[config.ENV_FAULT_ACTION] = "sleep"
    env[config.ENV_FAULT_ASSET] = args.asset
    env[config.ENV_FAULT_PARTITION] = args.partition
    env[config.ENV_FAULT_TOKEN] = "cancel-drill"
    env["REHEARSAL_ASSET"] = args.asset
    env["REHEARSAL_PARTITION"] = args.partition
    env["REHEARSAL_RUN_TAGS"] = json.dumps(
        {
            "rehearsal/asset": args.asset,
            "rehearsal/partition": args.partition,
            "rehearsal/reason": "cancel_drill",
            "rehearsal/request": args.request,
        }
    )

    proc = subprocess.Popen(
        [sys.executable, "-m", "rehearsal.run_one"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    run = wait_for_started(instance, args.asset, args.partition, timeout=30)
    if run is None:
        proc.kill()
        _print("cancel drill could not find a started run")
        return 1
    instance.report_run_canceling(run)
    proc.send_signal(signal.SIGINT)
    stdout = proc.communicate(timeout=90)[0]
    final = [
        r.dagster_run.status.value
        for r in instance.get_run_records(limit=200)
        if r.dagster_run.run_id == run.run_id
    ]

    payload = {
        "schema": "cancel_drill/1",
        "canceled_at": utc_now(),
        "asset": args.asset,
        "partition": args.partition,
        "run_id": run.run_id,
        "final_status": final[0] if final else "UNKNOWN",
        "exit_code": proc.returncode,
        "stdout_tail": stdout[-2000:],
        "published_trustworthy": sorted(lake.trustworthy_partitions(args.asset, [args.partition])),
        "staging": _staging_snapshot(),
    }
    path = _report("08_cancel", payload)
    _print(f"canceled run {run.run_id}: final_status={payload['final_status']}")
    _print(f"  artifact: {path}")
    return 0


def child_env() -> dict:
    env = dict(os.environ)
    env["DAGSTER_HOME"] = str(config.dagster_home())
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env[config.ENV_WORKSPACE] = str(config.workspace_root())
    return env


def cmd_demonstrate(args: argparse.Namespace) -> int:
    """Run the whole drill in order: generate, fail, recover, correct, verify.

    Every step writes a report; the sequence ends with the verification verdict.
    """
    _prepare()
    _prepare()
    steps = []
    total = 12

    def step(number: int, message: str) -> None:
        _print(f"[{number}/{total}] {message}")

    step(1, "generate sample data with scheduled gaps")
    steps.append(("generate", cli_step_generate()))
    step(2, "write the dependency map")
    steps.append(("dependency_map", _run_cli(["map"])))
    step(3, "initial plan and run")
    _print(f"  {cli_plan_summary()}")
    steps.append(("initial_run", _run_cli(["run", "--name", "03_initial_run"])))

    step(4, "fault drills at read / write / publish stages")
    for stage, asset, partition in _demo_faults():
        os.environ[config.ENV_FAULT_STAGE] = stage
        os.environ[config.ENV_FAULT_ACTION] = "fail"
        os.environ[config.ENV_FAULT_ASSET] = asset
        os.environ[config.ENV_FAULT_PARTITION] = partition
        steps.append(
            (
                f"fault_{stage}",
                _run_cli(["fail", "--stage", stage, "--asset", asset, "--partition", partition]),
            )
        )
        del os.environ[config.ENV_FAULT_STAGE]

    step(5, "restore upstream gaps")
    steps.append(("restore_gaps", _run_cli(["restore-gaps"])))
    step(6, "recover: quarantine partial material, recompute only what changed")
    steps.append(("recovery", _run_cli(["recover", "--name", "04_recovery"])))

    step(7, "cancel drill on a live run")
    os.environ["REHEARSAL_FAULT_SLEEP_SECONDS"] = "20"
    steps.append(
        (
            "cancel",
            _run_cli(
                [
                    "cancel",
                    "--asset",
                    config.ORDERS_WINDOW,
                    "--partition",
                    config.DEFAULT_PARTITIONS[0],
                ]
            ),
        )
    )
    os.environ.pop("REHEARSAL_FAULT_SLEEP_SECONDS", None)

    step(8, "apply a source correction")
    steps.append(("correction", _run_cli(["correct"])))
    step(9, "verify before repair: the correction must fail verification")
    pre_repair = _run_cli(["verify", "--name", "08_pre_repair_verification"])
    steps.append(("pre_repair_verification", pre_repair))
    if pre_repair != 0:
        _print("  expected: stale/mismatched results were detected, not silently accepted")

    step(10, "repair only the affected scope")
    steps.append(("scoped_repair", _run_cli(["repair-scope"])))

    step(11, "verify results against an independent baseline")
    verification = _run_cli(["verify"])
    step(12, "write the drill summary")

    summary = {
        "schema": "drill_summary/1",
        "workspace": str(config.workspace_root()),
        "steps": [{"step": name, "exit_code": code} for name, code in steps],
        "verification_exit": verification,
        "reports": sorted(p.name for p in config.reports_dir().glob("*.json")),
    }
    path = _report("00_drill_summary", summary)
    _print(f"drill complete: verification exit={verification}")
    _print(f"  summary: {path}")
    return verification


def cli_step_generate() -> int:
    return _run_cli(["generate"])


def cli_plan_summary() -> str:
    plan = planner.build_plan("initial_backfill")
    planner.write_plan(plan, "02_initial_plan")
    _report("02_initial_plan", plan)
    return (
        f"{plan['totals']['computable']} computable, "
        f"{plan['totals']['not_computable']} blocked by missing inputs"
    )


def _demo_faults() -> list[tuple[str, str, str]]:
    return [
        ("read", config.ORDERS_WINDOW, "2026-10-05"),
        ("write", config.SETTLEMENT, "2026-10-01"),
        ("publish", config.SETTLEMENT, "2026-10-02"),
    ]


def _run_cli(argv: list[str]) -> int:
    return main(argv)


def wait_for_started(instance: dg.DagsterInstance, asset: str, partition: str, timeout: int):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for record in instance.get_run_records(limit=100):
            run = record.dagster_run
            if (
                run.tags.get("rehearsal/asset") == asset
                and run.tags.get("rehearsal/partition") == partition
                and run.status == dg.DagsterRunStatus.STARTED
            ):
                return run
        time.sleep(0.5)
    return None


def cmd_verify(args: argparse.Namespace) -> int:
    _prepare()
    report = verify.verify(config.DEFAULT_PARTITIONS)
    path = _report(args.name, report)
    _print(f"verification passed={report['passed']} totals={report['totals']}")
    if report["findings"]:
        for finding in report["findings"][:8]:
            _print(
                f"  {finding['asset']}/{finding['partition']}: {finding['status']} {finding['detail']}"
            )
    _print(f"  artifact: {path}")
    return 0 if report["passed"] else 1


def cmd_map(_args: argparse.Namespace) -> int:
    _prepare()
    path = write_dependency_map(config.DEFAULT_PARTITIONS)
    _print(f"dependency map: {path}")
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    _prepare()
    published = {
        asset: sorted(lake.published_partitions(asset, config.DEFAULT_PARTITIONS))
        for asset in config.DERIVED_ASSETS
    }
    sources = {
        asset: sorted(lake.present_partitions(asset, config.DEFAULT_PARTITIONS))
        for asset in config.SOURCE_ASSETS
    }
    _print(f"workspace: {config.workspace_root()}")
    for asset, partitions in sources.items():
        _print(f"  source {asset}: {len(partitions)}/{len(config.DEFAULT_PARTITIONS)} present")
    for asset, partitions in published.items():
        _print(f"  derived {asset}: {len(partitions)}/{len(config.DEFAULT_PARTITIONS)} published")
    return 0


def cmd_up(args: argparse.Namespace) -> int:
    """Start the Dagster UI against this case, for interactive inspection."""
    _prepare()
    if not args.background:
        env = child_env()
        cmd = [
            sys.executable,
            "-m",
            "dagster_webserver",
            "-m",
            "rehearsal.assets",
            "-p",
            str(args.port),
        ]
        _print(f"starting webserver on port {args.port} (workspace {config.workspace_root()})")
        return subprocess.call(cmd, env=env)

    pid_path = config.state_dir() / "webserver.pid"
    log_path = config.state_dir() / "webserver.log"
    if pid_path.exists():
        _print(f"webserver already running (pid {pid_path.read_text().strip()}); run `down` first")
        return 1
    env = child_env()
    log_handle = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "dagster_webserver", "-m", "rehearsal.assets", "-p", str(args.port)],
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    write_json_atomic(pid_path, {"pid": proc.pid, "port": args.port, "started_at": utc_now()})
    _print(f"webserver started in background: pid={proc.pid} port={args.port}")
    _print(f"  url: http://localhost:{args.port}")
    _print(f"  log: {log_path}")
    return 0


def cmd_down(_args: argparse.Namespace) -> int:
    """Stop a background webserver started with ``up --background``."""
    _prepare()
    pid_path = config.state_dir() / "webserver.pid"
    if not pid_path.exists():
        _print("no background webserver recorded")
        return 0
    record = read_json(pid_path)
    pid = record.get("pid")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        _print(f"webserver process {pid} already gone")
    pid_path.unlink(missing_ok=True)
    _print(f"stopped webserver pid={pid}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rehearsal", description="dependent-backfill rehearsal")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="create initial sample data with scheduled gaps")
    generate.set_defaults(func=cmd_generate)

    gap = sub.add_parser("restore-gaps", help="deliver the withheld upstream partitions")
    gap.set_defaults(func=cmd_restore_gaps)

    plan = sub.add_parser("plan", help="evaluate which partitions are computable")
    plan.add_argument("--reason", default="initial_backfill")
    plan.add_argument("--name", default="02_initial_plan")
    plan.set_defaults(func=cmd_plan)

    run = sub.add_parser("run", help="execute the computable partitions of a plan")
    run.add_argument("--reason", default="initial_backfill")
    run.add_argument("--name", default="03_initial_run")
    run.add_argument("--request")
    run.set_defaults(func=cmd_run)

    fail = sub.add_parser("fail", help="inject a controllable interruption for one work item")
    fail.add_argument("--stage", choices=config.FAULT_STAGES, required=True)
    fail.add_argument("--action", choices=["fail", "signal", "sleep"], default="fail")
    fail.add_argument("--asset", required=True)
    fail.add_argument("--partition", required=True)
    fail.add_argument("--request", default="fault-drill")
    fail.add_argument("--token", default="drill")
    fail.set_defaults(func=cmd_fail)

    recover = sub.add_parser(
        "recover", help="quarantine partial material and recompute the remainder"
    )
    recover.add_argument("--scope", default="gap_fill")
    recover.add_argument("--name", default="04_recovery")
    recover.add_argument("--request")
    recover.set_defaults(func=cmd_recover)

    correct = sub.add_parser("correct", help="append a corrected source revision")
    correct.add_argument("--asset", default=config.RAW_ORDERS)
    correct.add_argument("--partition", default=config.CORRECTION_TARGET)
    correct.add_argument("--salt", default="correction-v2")
    correct.set_defaults(func=cmd_correct)

    scope_cmd = sub.add_parser("repair-scope", help="recompute only the corrected scope")
    scope_cmd.add_argument("--scope", default="post_correction")
    scope_cmd.add_argument("--name", default="07_scoped_repair")
    scope_cmd.add_argument("--request")
    scope_cmd.set_defaults(func=cmd_repair_scope)

    cancel = sub.add_parser("cancel", help="start a run and cancel it mid-flight")
    cancel.add_argument("--asset", default=config.SETTLEMENT)
    cancel.add_argument("--partition", default=config.DEFAULT_PARTITIONS[-1])
    cancel.add_argument("--request", default="cancel-drill")
    cancel.set_defaults(func=cmd_cancel)

    verify_cmd = sub.add_parser("verify", help="compare results against an independent baseline")
    verify_cmd.add_argument("--name", default="09_verification")
    verify_cmd.set_defaults(func=cmd_verify)

    demo = sub.add_parser("demonstrate", help="run the entire drill end to end")
    demo.set_defaults(func=cmd_demonstrate)

    map_cmd = sub.add_parser("map", help="write the dependency map artifact")
    map_cmd.set_defaults(func=cmd_map)

    status = sub.add_parser("status", help="summarize what exists in the workspace")
    status.set_defaults(func=cmd_status)

    up = sub.add_parser("up", help="run the Dagster webserver for this case")
    up.add_argument("--port", type=int, default=3001)
    up.add_argument(
        "--background", action="store_true", help="detach the webserver and record its pid"
    )
    up.set_defaults(func=cmd_up)

    down = sub.add_parser("down", help="stop the background webserver")
    down.set_defaults(func=cmd_down)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
