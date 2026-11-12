"""Computation for derived assets, with controllable interruptions at each stage.

The three stages are deliberately separated:

``read``
    Selected inputs are staged, then the attempt stops. Nothing is published, but
    the staging directory keeps a real input manifest — a partial artifact.
``write``
    The manifest and computed values are staged, then the attempt stops before any
    commit marker. This leaves more material than ``read`` but is still not a
    result.
``publish``
    A published file is written *before* the commit marker, then the attempt stops.
    This is the dangerous case: the result looks present on disk while the run that
    produced it never recorded success.
"""

import os
import time

import dagster as dg

from . import config, lake
from .provenance import content_hash


def _maybe_interrupt(stage: str, asset: str, partition: str) -> None:
    fault = config.read_fault()
    if not config.fault_applies(fault, asset, partition) or fault.stage != stage:
        return
    metadata = {"asset": asset, "partition": partition, "stage": stage, "action": fault.action}
    if fault.action == "fail":
        raise dg.Failure(
            description=f"injected failure at {stage} for {asset}/{partition}", metadata=metadata
        )
    if fault.action == "signal":
        os.kill(os.getpid(), 2)  # SIGINT: exercise the real interruption path
    if fault.action == "sleep":
        # Hold the attempt open so an operator (or the cancel drill) can cancel it
        # from outside. Cancelling mid-sleep raises and leaves the staged inputs in
        # place, which is exactly the state recovery has to recognise.
        time.sleep(float(os.environ.get("REHEARSAL_FAULT_SLEEP_SECONDS", "60")))
        raise dg.Failure(
            description=f"injected interrupt at {stage} for {asset}/{partition}", metadata=metadata
        )
    raise dg.Failure(
        description=f"injected interrupt at {stage} for {asset}/{partition}", metadata=metadata
    )


def _upstream_window(partition: str) -> list[str]:
    """Partitions of raw_orders that orders_window_7d/<partition> consumes.

    Dagster clamps a time window at the start of the partition definition, so the
    first few days pull fewer than seven upstream partitions. We reproduce that
    same clamping here instead of assuming a constant window.
    """
    from .assets import ORDERS_WINDOW_MAPPING, daily

    subset = daily.empty_subset().with_partition_keys([partition])
    result = ORDERS_WINDOW_MAPPING.get_upstream_mapped_partitions_result_for_partitions(
        subset, daily, daily
    )
    return result.partitions_subset.get_partition_keys()


def _materialize(
    asset: str,
    partition: str,
    attempt_id: str,
    manifest_entries: list[dict],
    values: dict,
    stage_write: bool,
    publish_before_commit: bool,
) -> dg.MaterializeResult:
    lake.stage_input_manifest(asset, partition, attempt_id, {"entries": manifest_entries})
    _maybe_interrupt("read", asset, partition)

    if stage_write:
        lake.stage_values(asset, partition, attempt_id, values)
    _maybe_interrupt("write", asset, partition)

    result = {
        "schema": "derived_result/1",
        "asset": asset,
        "partition": partition,
        "attempt_id": attempt_id,
        "committed": True,
        "values": values,
        "inputs": [
            {"asset": entry["asset"], "partition": entry["partition"], "version": entry["version"]}
            for entry in manifest_entries
        ],
        "input_fingerprint": lake.fingerprint_inputs(manifest_entries),
        "values_hash": content_hash(values),
    }

    if publish_before_commit:
        # A provisional file lands on disk before the commit marker. If the process
        # dies here, a result-shaped file exists that was never committed; it is
        # written with committed=False so verification can name it rather than
        # silently accept it.
        lake.publish_result(asset, partition, {**result, "committed": False})
        _maybe_interrupt("publish", asset, partition)

    lake.stage_commit(asset, partition, attempt_id)
    previous = lake.read_published(asset, partition)
    republished = True
    if (
        previous
        and previous.get("committed")
        and previous.get("input_fingerprint") == result["input_fingerprint"]
        and previous.get("values_hash") == result["values_hash"]
    ):
        # Identical inputs, identical values: publishing again would replace a
        # result with the same content and make repeat requests look like new work.
        republished = False
    else:
        lake.publish_result(asset, partition, result)

    return dg.MaterializeResult(
        data_version=dg.DataVersion(result["values_hash"]),
        metadata={
            "rehearsal/role": "derived",
            "rehearsal/asset": asset,
            "rehearsal/partition": partition,
            "rehearsal/attempt_id": attempt_id,
            "rehearsal/input_fingerprint": result["input_fingerprint"],
            "rehearsal/values_hash": result["values_hash"],
            "rehearsal/republished": republished,
            "rehearsal/inputs": dg.MetadataValue.json(result["inputs"]),
        },
    )


def compute_orders_window(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    partition = context.partition_key
    window = _upstream_window(partition)

    entries = []
    missing = []
    for upstream_partition in window:
        seq = lake.latest_seq(config.RAW_ORDERS, upstream_partition)
        if seq is None:
            missing.append(upstream_partition)
            continue
        record = lake.read_revision(config.RAW_ORDERS, upstream_partition, seq)
        entries.append(
            {
                "asset": config.RAW_ORDERS,
                "partition": upstream_partition,
                "seq": seq,
                "version": f"seq-{seq}",
                "revision_label": record["revision_label"],
                "values": record["values"],
            }
        )

    if missing:
        raise dg.Failure(
            description=f"missing raw_orders partitions for window of {partition}: {missing}",
            metadata={"asset": config.ORDERS_WINDOW, "partition": partition, "missing": missing},
        )

    # Values are pure data derived from the inputs' *contents*. Which revision
    # produced them is provenance, recorded separately in ``inputs``. Keeping the
    # two apart is what lets verification say "same numbers, wrong revision".
    gross = round(sum(entry["values"]["gross_amount"] for entry in entries), 2)
    demand = round(sum(entry["values"]["order_count"] * 10.0 for entry in entries), 2)
    attempt_id = context.run_id
    values = {
        "window_partitions": [entry["partition"] for entry in entries],
        "rolling_amount": gross,
        "demand_index": demand,
    }
    result = _materialize(
        config.ORDERS_WINDOW,
        partition,
        attempt_id,
        entries,
        values,
        stage_write=True,
        publish_before_commit=False,
    )
    return result


def compute_settlement(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    partition = context.partition_key
    attempt_id = context.run_id

    fx_seq = lake.latest_seq(config.RAW_FX, partition)
    if fx_seq is None:
        raise dg.Failure(description=f"missing {config.RAW_FX}/{partition}")
    if not lake.trustworthy_partitions(config.ORDERS_WINDOW, [partition]):
        raise dg.Failure(description=f"no committed result for {config.ORDERS_WINDOW}/{partition}")
    window_published = lake.read_published(config.ORDERS_WINDOW, partition)
    if window_published is None:
        raise dg.Failure(
            description=f"missing published result for {config.ORDERS_WINDOW}/{partition}"
        )

    fx_record = lake.read_revision(config.RAW_FX, partition, fx_seq)
    fx_rate = fx_record["values"]["usd_per_unit"]
    rolling = window_published["values"]["rolling_amount"]
    settled = round(rolling * fx_rate, 2)

    entries = [
        {
            "asset": config.ORDERS_WINDOW,
            "partition": partition,
            "version": window_published["values_hash"],
        },
        {
            "asset": config.RAW_FX,
            "partition": partition,
            "seq": fx_seq,
            "version": f"seq-{fx_seq}",
            "revision_label": fx_record["revision_label"],
        },
    ]
    values = {
        "rolling_amount": rolling,
        "usd_per_unit": fx_rate,
        "settled_usd": settled,
        "window_input_fingerprint": window_published["input_fingerprint"],
    }
    return _materialize(
        config.SETTLEMENT,
        partition,
        attempt_id,
        entries,
        values,
        stage_write=True,
        publish_before_commit=True,
    )
