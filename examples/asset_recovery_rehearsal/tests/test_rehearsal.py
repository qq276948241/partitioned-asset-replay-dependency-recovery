"""End-to-end tests for the backfill rehearsal, run against a real local Dagster instance."""

import json
import os
import subprocess
import sys

import pytest

from rehearsal import cli, config, lake, planner, sample_data, verify
from rehearsal.provenance import write_json_atomic


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    """A disposable workspace with its own DAGSTER_HOME and empty lake."""
    monkeypatch.setenv(config.ENV_WORKSPACE, str(tmp_path / "ws"))
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "ws" / "dagster_home"))
    for directory in config.all_directories():
        directory.mkdir(parents=True, exist_ok=True)
    write_json_atomic(config.dagster_home() / "dagster.yaml", {})
    yield tmp_path / "ws"
    for variable in (
        config.ENV_FAULT_STAGE,
        config.ENV_FAULT_ACTION,
        config.ENV_FAULT_ASSET,
        config.ENV_FAULT_PARTITION,
        config.ENV_FAULT_TOKEN,
        "REHEARSAL_FAULT_SLEEP_SECONDS",
    ):
        monkeypatch.delenv(variable, raising=False)


def _generate(partitions=None):
    return sample_data.generate_initial(
        partitions or config.DEFAULT_PARTITIONS,
        config.INITIAL_MISSING_ORDERS,
        config.INITIAL_MISSING_FX,
    )


def run_plan(plan, request_id):
    return cli.run_plan(plan, request_id)


def test_missing_partitions_are_not_computed(workspace):
    """A gap in the source window must block every downstream partition that needs it."""
    _generate()
    plan = planner.build_plan("gap_check")

    blocked = {(e["asset"], e["partition"]) for e in plan["not_computable"]}
    # orders_window_7d/2026-10-04 reads raw_orders/2026-10-04, which is withheld.
    assert ("orders_window_7d", "2026-10-04") in blocked
    missing = next(
        e
        for e in plan["not_computable"]
        if e["asset"] == config.ORDERS_WINDOW and e["partition"] == "2026-10-04"
    )
    assert {"asset": config.RAW_ORDERS, "partition": "2026-10-04"} in missing["missing_inputs"]

    result = run_plan(plan, "req-gap")
    assert result["failed"] == 0
    # Every launched partition succeeded, and no blocked partition was launched.
    launched = {(o["asset"], o["partition"]) for o in result["outcomes"]}
    for asset in config.DERIVED_ASSETS:
        assert not any(key[0] == asset and key in blocked for key in launched)

    # A blocked partition has no published result at all.
    assert lake.read_published(config.ORDERS_WINDOW, "2026-10-04") is None


def test_gap_fill_recovery_is_scoped(workspace):
    """Restoring the gaps should let recovery compute exactly what was blocked."""
    _generate()
    run_plan(planner.build_plan("initial"), "req-first")
    published_before = {
        (asset, partition)
        for asset in config.DERIVED_ASSETS
        for partition in config.DEFAULT_PARTITIONS
        if lake.trustworthy_partitions(asset, [partition])
    }

    sample_data.restore_gaps(
        config.DEFAULT_PARTITIONS, config.INITIAL_MISSING_ORDERS, config.INITIAL_MISSING_FX
    )
    recovery_plan = planner.build_plan("gap_fill")
    recovered = run_plan(recovery_plan, "req-recover")

    newly = {
        (o["asset"], o["partition"])
        for o in recovered["outcomes"]
        if o.get("run_status") == "SUCCESS"
    }
    # Recovery must not recompute results that were already current.
    assert newly.isdisjoint(published_before)
    # The blocked 10-04 window must now exist, and so must its settlement.
    assert ("orders_window_7d", "2026-10-04") in newly
    assert lake.trustworthy_partitions(config.ORDERS_WINDOW, ["2026-10-04"])

    report = verify.verify(config.DEFAULT_PARTITIONS)
    assert report["passed"], report["findings"]


def test_stale_input_detected_even_when_values_match(workspace):
    """A run that succeeded against an older revision must be caught by provenance,
    even if that old revision happened to produce identical numbers.

    This is the failure mode a green orchestration UI hides: the run succeeded,
    the value comparison passes, but the result consumed a superseded revision.
    """
    _generate()
    run_plan(planner.build_plan("initial"), "req-initial")
    sample_data.restore_gaps(
        config.DEFAULT_PARTITIONS, config.INITIAL_MISSING_ORDERS, config.INITIAL_MISSING_FX
    )
    run_plan(planner.build_plan("gap_fill"), "req-gapfill")

    # Re-publish a corrected revision whose *values* are identical to the previous
    # one (same salt), so only the revision sequence number moves.
    asset, partition = config.RAW_ORDERS, "2026-10-02"
    before_seq = lake.latest_seq(asset, partition)
    before_values = lake.read_revision(asset, partition, before_seq)["values"]
    sample_data.correct_partition(config.DEFAULT_PARTITIONS, asset, partition, "initial")
    after_seq = lake.latest_seq(asset, partition)
    assert after_seq == before_seq + 1
    assert lake.read_revision(asset, partition, after_seq)["values"] == before_values

    report = verify.verify(config.DEFAULT_PARTITIONS)
    # The downstream window still carries the *old* seq and identical values,
    # so it must be flagged for stale inputs rather than passing silently.
    windows_rows = [
        row
        for row in report["rows"]
        if row["asset"] == config.ORDERS_WINDOW and row["partition"] == "2026-10-02"
    ]
    assert windows_rows
    row = windows_rows[0]
    assert row["status"] == "stale_input"
    assert row["value_match"] is True
    assert any(stale["input"] == f"{asset}/{partition}" for stale in row["stale_inputs"])


def test_repeat_request_does_not_republish(workspace):
    """The same request issued twice must not publish a second time."""
    _generate()
    plan = planner.build_plan("initial")
    first = run_plan(plan, "req-dup")
    assert first["succeeded"] > 0

    stamps = {
        (asset, partition): lake.published_path(asset, partition).stat().st_mtime_ns
        for asset in config.DERIVED_ASSETS
        for partition in config.DEFAULT_PARTITIONS
        if lake.published_path(asset, partition).exists()
    }

    second_plan = planner.build_plan("initial")
    second = run_plan(second_plan, "req-dup")
    assert second["attempted"] == 0  # nothing left to compute
    assert second_plan["totals"]["computable"] == 0

    new_stamps = {
        (asset, partition): lake.published_path(asset, partition).stat().st_mtime_ns
        for asset in config.DERIVED_ASSETS
        for partition in config.DEFAULT_PARTITIONS
        if lake.published_path(asset, partition).exists()
    }
    assert new_stamps == stamps  # untouched files, not rewritten with identical bytes


def test_canceled_run_does_not_advance_partition(workspace):
    """Cancelling mid-run leaves no committed result and a real CANCELED record."""
    _generate()

    # Cancel a partition that has never produced a result, so the cancel is
    # observable: orders_window_7d/2026-10-01 is computable on first pass.
    asset, partition = config.ORDERS_WINDOW, "2026-10-01"
    assert lake.read_published(asset, partition) is None

    env = cli.child_env()
    env[config.ENV_FAULT_STAGE] = "read"
    env[config.ENV_FAULT_ACTION] = "sleep"
    env[config.ENV_FAULT_ASSET] = asset
    env[config.ENV_FAULT_PARTITION] = partition
    env["REHEARSAL_ASSET"] = asset
    env["REHEARSAL_PARTITION"] = partition
    env["REHEARSAL_FAULT_SLEEP_SECONDS"] = "30"
    env["REHEARSAL_RUN_TAGS"] = json.dumps(
        {
            "rehearsal/asset": asset,
            "rehearsal/partition": partition,
            "rehearsal/request": "req-cancel",
        }
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "rehearsal.run_one"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    import dagster as dg

    instance = dg.DagsterInstance.get()
    run = cli.wait_for_started(instance, asset, partition, timeout=30)
    assert run is not None
    instance.report_run_canceling(run)
    proc.send_signal(2)
    proc.communicate(timeout=60)

    records = [r for r in instance.get_run_records(limit=200) if r.dagster_run.run_id == run.run_id]
    assert records and records[0].dagster_run.status == dg.DagsterRunStatus.CANCELED
    # The canceled attempt must not have published a trustworthy result.
    assert lake.read_published(asset, partition) is None
    assert not lake.trustworthy_partitions(asset, [partition])

    # Quarantine removes the partial material, and recovery can compute it cleanly.
    moved = lake.quarantine_partial_attempts()
    assert any(row["asset"] == asset and row["partition"] == partition for row in moved)
    plan = planner.build_plan("post_cancel")
    entry = next(
        e for e in plan["computable"] if e["asset"] == asset and e["partition"] == partition
    )
    recovered = run_plan(
        {"reason": "post_cancel", "plan_id": "manual", "computable": [entry]}, "req-after-cancel"
    )
    assert recovered["succeeded"] == 1
    assert lake.trustworthy_partitions(asset, [partition])


def test_stale_revision_is_detected_and_repaired(workspace):
    """After a source correction, verification flags stale inputs; scoped repair fixes them."""
    _generate()
    run_plan(planner.build_plan("initial"), "req-initial")
    sample_data.restore_gaps(
        config.DEFAULT_PARTITIONS, config.INITIAL_MISSING_ORDERS, config.INITIAL_MISSING_FX
    )
    run_plan(planner.build_plan("gap_fill"), "req-gapfill")
    assert verify.verify(config.DEFAULT_PARTITIONS)["passed"]

    changed = sample_data.correct_partition(
        config.DEFAULT_PARTITIONS, config.RAW_ORDERS, config.CORRECTION_TARGET, "correction-v2"
    )
    assert changed["seq"] == changed["previous_seq"] + 1

    report = verify.verify(config.DEFAULT_PARTITIONS)
    assert not report["passed"]
    assert report["totals"].get("value_mismatch", 0) > 0

    scope = planner.affected_scope(
        [{"asset": config.RAW_ORDERS, "partition": config.CORRECTION_TARGET}],
        config.DEFAULT_PARTITIONS,
    )
    repair_plan = planner.scope_plan("correction", scope, config.DEFAULT_PARTITIONS)
    repaired = run_plan(repair_plan, "req-repair")
    assert repaired["failed"] == 0

    final = verify.verify(config.DEFAULT_PARTITIONS)
    assert final["passed"], final["findings"]
    # Only the affected scope was touched; unaffected windows were never recomputed.
    touched = {(o["asset"], o["partition"]) for o in repaired["outcomes"]}
    affected = {(item["asset"], item["partition"]) for item in scope["affected"]}
    assert touched <= affected


def test_failed_upstream_skips_downstream_sibling(workspace):
    """If a window planned in this pass fails, its settlement sibling is skipped.

    This is the run-level counterpart of the cancel drill: a failure partway
    through a plan must stop the chain, not let a downstream partition run against
    the previous (now stale) window.
    """
    os.environ[config.ENV_FAULT_STAGE] = "write"
    os.environ[config.ENV_FAULT_ACTION] = "fail"
    os.environ[config.ENV_FAULT_ASSET] = config.ORDERS_WINDOW
    os.environ[config.ENV_FAULT_PARTITION] = "2026-10-01"
    _generate()
    result = run_plan(planner.build_plan("initial"), "req-chain")

    failed_window = next(
        o
        for o in result["outcomes"]
        if o["asset"] == config.ORDERS_WINDOW and o["partition"] == "2026-10-01"
    )
    assert failed_window["run_status"] == "FAILURE"

    settlement = next(
        o
        for o in result["outcomes"]
        if o["asset"] == config.SETTLEMENT and o["partition"] == "2026-10-01"
    )
    assert settlement["run_status"] == "SKIPPED"
    assert settlement["skipped_because"] == [f"{config.ORDERS_WINDOW}/2026-10-01"]
    # The settlement never ran, so it has no published result.
    assert lake.read_published(config.SETTLEMENT, "2026-10-01") is None


def test_untrusted_published_file_blocks_downstream(workspace):
    """A file that exists but was never committed must not be consumed downstream."""
    os.environ[config.ENV_FAULT_STAGE] = "publish"
    os.environ[config.ENV_FAULT_ACTION] = "fail"
    os.environ[config.ENV_FAULT_ASSET] = config.SETTLEMENT
    os.environ[config.ENV_FAULT_PARTITION] = "2026-10-03"
    _generate()
    run_plan(planner.build_plan("initial"), "req-pubs")

    assert "2026-10-03" in lake.untrusted_published_files(
        config.SETTLEMENT, config.DEFAULT_PARTITIONS
    )
    assert not lake.trustworthy_partitions(config.SETTLEMENT, ["2026-10-03"])

    report = verify.verify(config.DEFAULT_PARTITIONS)
    statuses = {row["status"] for row in report["rows"] if row["partition"] == "2026-10-03"}
    assert "untrusted_file" in statuses

    del os.environ[config.ENV_FAULT_STAGE]
    plan = planner.build_plan("after_publish_fault")
    entry = next(
        e
        for e in plan["computable"]
        if e["asset"] == config.SETTLEMENT and e["partition"] == "2026-10-03"
    )
    recovered = run_plan(
        {"reason": "after_publish_fault", "plan_id": "manual", "computable": [entry]}, "req-fix"
    )
    assert recovered["succeeded"] == 1
    assert lake.trustworthy_partitions(config.SETTLEMENT, ["2026-10-03"])


def test_untrusted_upstream_window_blocks_downstream(workspace):
    """A settlement must not consume an upstream window whose file is uncommitted.

    This is the "half-finished material is not a successful input" rule at the
    graph level: not just that the bad file is flagged, but that nothing
    downstream is planned or allowed to read it.
    """
    import json

    _generate()
    run_plan(planner.build_plan("initial"), "req-warm")
    assert lake.trustworthy_partitions(config.ORDERS_WINDOW, ["2026-10-01"])

    # Simulate an interrupted republish of the window: the file survives with its
    # data intact but is no longer a committed result.
    path = lake.published_path(config.ORDERS_WINDOW, "2026-10-01")
    payload = json.loads(path.read_text())
    payload["committed"] = False
    path.write_text(json.dumps(payload))

    assert not lake.trustworthy_partitions(config.ORDERS_WINDOW, ["2026-10-01"])
    entry = planner.evaluate_partition(config.SETTLEMENT, "2026-10-01")
    assert entry["status"] == "not_computable"
    assert {"asset": config.ORDERS_WINDOW, "partition": "2026-10-01"} in entry["missing_inputs"]

    # And the compute layer refuses it too, independently of the planner.
    from rehearsal import computes

    class _Ctx:
        partition_key = "2026-10-01"
        run_id = "manual-attempt"
        log = None

    with pytest.raises(Exception):
        computes.compute_settlement(_Ctx())


def test_dependency_map_records_boundaries_and_rules(workspace):
    """The map artifact must expose clamped windows and the three rules."""
    _generate()
    from rehearsal.depmap import build_dependency_map

    dep_map = build_dependency_map(config.DEFAULT_PARTITIONS)
    first_day = next(c for c in dep_map["start_boundary_clamped"] if c["partition"] == "2026-10-01")
    assert first_day["resolved_window"] == ["2026-10-01"]
    assert first_day["nominal_size"] == 7
    assert set(dep_map["rules"]) == {"start_boundary", "missing_input", "correction"}
    edges = {(e["downstream"], e["upstream"]) for e in dep_map["edges"]}
    assert (config.ORDERS_WINDOW, config.RAW_ORDERS) in edges
    assert (config.SETTLEMENT, config.ORDERS_WINDOW) in edges
    assert (config.SETTLEMENT, config.RAW_FX) in edges


def test_cli_drill_produces_reports(workspace, capsys):
    """The operator commands write the artifacts the README promises."""
    assert cli.main(["generate"]) == 0
    assert cli.main(["plan"]) == 0
    assert cli.main(["run"]) == 0
    assert cli.main(["restore-gaps"]) == 0
    assert cli.main(["recover"]) == 0
    assert cli.main(["correct"]) == 0
    assert cli.main(["repair-scope"]) == 0
    verification = cli.main(["verify"])

    for name in (
        "01_sample_generation",
        "03_initial_run",
        "06_gap_restoration",
        "07_correction",
        "09_verification",
    ):
        assert (config.reports_dir() / f"{name}.json").exists()
    assert verification == 0
