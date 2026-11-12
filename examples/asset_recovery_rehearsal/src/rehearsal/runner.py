"""Execute a plan against real Dagster runs and record the outcome.

Each work item is a separate Dagster run so run records line up one-to-one with
partition decisions. Before launching, the runner asks why the partition is
being computed and which revisions it will read; those answers are attached to
the run as tags and mirrored into the journal.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import dagster as dg

from . import config, lake
from .assets import ALL_ASSETS
from .provenance import append_jsonl, content_hash, stable_id, utc_now

RUNNER_MODULE = "rehearsal.run_one"


def _asset_def(asset: str) -> dg.AssetsDefinition:
    for candidate in ALL_ASSETS:
        if candidate.key.to_user_string() == asset:
            return candidate
    raise KeyError(f"unknown asset {asset}")


def _reason_for(asset: str, partition: str, plan_reason: str) -> str:
    if plan_reason.startswith("correction"):
        return "correction"
    if plan_reason.startswith("gap"):
        return "gap_fill"
    return plan_reason


def _input_summary(entry: dict) -> dict:
    inputs = []
    for item in entry["resolved_inputs"]:
        if item.get("committed"):
            payload = lake.read_published(item["asset"], item["partition"]) or {}
            inputs.append(
                {
                    "asset": item["asset"],
                    "partition": item["partition"],
                    "version": payload.get("values_hash"),
                }
            )
        else:
            seq = item.get("seq")
            inputs.append(
                {
                    "asset": item["asset"],
                    "partition": item["partition"],
                    "version": None if seq is None else f"seq-{seq}",
                }
            )
    return {"inputs": inputs, "input_fingerprint": content_hash(inputs)}


def launch_work_item(
    asset: str,
    partition: str,
    reason: str,
    request_id: str,
) -> dict:
    """Run one partition in a child process and return its observed outcome."""
    _asset_def(asset)  # fail fast on an unknown asset before launching anything
    tags = {
        "rehearsal/asset": asset,
        "rehearsal/partition": partition,
        "rehearsal/reason": reason,
        "rehearsal/request": request_id,
    }

    env = dict(os.environ)
    env["DAGSTER_HOME"] = str(config.dagster_home())
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env[config.ENV_WORKSPACE] = str(config.workspace_root())
    env["REHEARSAL_RUN_TAGS"] = json.dumps(tags)
    env["REHEARSAL_ASSET"] = asset
    env["REHEARSAL_PARTITION"] = partition

    cmd = [sys.executable, "-m", RUNNER_MODULE]
    child = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300, check=False)

    outcome = {
        "asset": asset,
        "partition": partition,
        "reason": reason,
        "request_id": request_id,
        "exit_code": child.returncode,
        "stdout_tail": child.stdout[-4000:],
        "stderr_tail": child.stderr[-4000:],
    }
    outcome.update(_observe_run(asset, partition, request_id))
    return outcome


def _tagged_runs(asset: str, partition: str, request_id: str) -> list[dg.RunRecord]:
    instance = dg.DagsterInstance.get()
    matches = []
    for record in instance.get_run_records(limit=200):
        run_tags = record.dagster_run.tags
        if (
            run_tags.get("rehearsal/asset") == asset
            and run_tags.get("rehearsal/partition") == partition
            and run_tags.get("rehearsal/request") == request_id
        ):
            matches.append(record)
    return matches


def _observe_run(asset: str, partition: str, request_id: str) -> dict:
    """Read what actually happened from the Dagster instance, not from stdout."""
    instance = dg.DagsterInstance.get()
    records = _tagged_runs(asset, partition, request_id)
    if not records:
        return {"run_id": None, "run_status": "NOT_RECORDED", "materialized": False}
    run = records[0].dagster_run
    materialized = False
    asset_key = dg.AssetKey(asset)
    fetched = instance.fetch_materializations(
        dg.AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition]), limit=50
    )
    for record in fetched.records:
        if record.run_id == run.run_id:
            materialized = True
            break
    return {"run_id": run.run_id, "run_status": run.status.value, "materialized": materialized}


def _journal_path() -> Path:
    return config.journal_dir() / "runs.jsonl"


def settle_published(request_id: str, outcomes: list[dict]) -> list[dict]:
    """Mark attempts that produced a trustworthy published result."""
    accepted = []
    for outcome in outcomes:
        if outcome.get("run_status") != "SUCCESS":
            continue
        if outcome["asset"] not in config.DERIVED_ASSETS:
            continue
        payload = lake.read_published(outcome["asset"], outcome["partition"])
        if payload and payload.get("committed"):
            accepted.append(
                {
                    "asset": outcome["asset"],
                    "partition": outcome["partition"],
                    "values_hash": payload["values_hash"],
                    "request_id": request_id,
                }
            )
    return accepted


def record_outcomes(outcomes: list[dict], plan: dict, request_id: str) -> dict:
    rows = []
    for outcome in outcomes:
        row = {
            "schema": "run_journal/1",
            "recorded_at": utc_now(),
            "request_id": request_id,
            "plan_id": plan["plan_id"],
            **outcome,
        }
        append_jsonl(_journal_path(), row)
        rows.append(row)
    return {"request_id": request_id, "records": len(rows)}


def execute_plan(plan: dict, request_id: str) -> dict:
    """Execute computable entries asset by asset, never letting a later item run
    on an upstream whose planned recompute did not land.

    A partition scheduled earlier in this plan is a dependency of anything
    downstream of it. If that item fails or is canceled, downstream items that
    were counting on it are skipped, not launched against the stale result.
    """
    started = utc_now()
    outcomes: list[dict] = []
    planned_ok: dict[tuple[str, str], bool] = {}

    for asset in config.DERIVED_ASSETS:
        entries = [e for e in plan.get("computable", []) if e["asset"] == asset]
        for entry in entries:
            partition = entry["partition"]
            blocking = [
                f"{item['asset']}/{item['partition']}"
                for item in entry.get("resolved_inputs", [])
                if item.get("scheduled")
                and not planned_ok.get((item["asset"], item["partition"]), True)
            ]
            if blocking:
                outcomes.append(
                    {
                        "asset": asset,
                        "partition": partition,
                        "reason": _reason_for(asset, partition, plan["reason"]),
                        "request_id": request_id,
                        "exit_code": None,
                        "run_id": None,
                        "run_status": "SKIPPED",
                        "materialized": False,
                        "skipped_because": blocking,
                    }
                )
                planned_ok[(asset, partition)] = False
                continue

            reason = _reason_for(asset, partition, plan["reason"])
            outcome = launch_work_item(asset, partition, reason, request_id)
            outcomes.append(outcome)
            planned_ok[(asset, partition)] = outcome.get("run_status") == "SUCCESS"

    accepted = settle_published(request_id, outcomes)
    result = {
        "schema": "execution_result/1",
        "request_id": request_id,
        "plan_id": plan["plan_id"],
        "reason": plan["reason"],
        "started_at": started,
        "finished_at": utc_now(),
        "attempted": sum(1 for o in outcomes if o.get("run_status") != "SKIPPED"),
        "succeeded": sum(1 for o in outcomes if o.get("run_status") == "SUCCESS"),
        "failed": sum(1 for o in outcomes if o.get("run_status") not in ("SUCCESS", "SKIPPED")),
        "canceled": sum(1 for o in outcomes if o.get("run_status") == "CANCELED"),
        "skipped": sum(1 for o in outcomes if o.get("run_status") == "SKIPPED"),
        "accepted_results": accepted,
        "outcomes": outcomes,
    }
    result["result_id"] = stable_id("result", {"request": request_id, "plan": plan["plan_id"]})
    return result
