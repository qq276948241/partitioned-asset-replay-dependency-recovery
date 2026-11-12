"""Independent recomputation and item-by-item comparison against the published lake.

The baseline is computed from source revisions directly, without running Dagster
and without reading any published file. Comparisons check values *and* the source
revisions each result claims to have read, so a run that succeeded against stale
inputs is still caught.
"""

from . import config, lake
from .depmap import upstream_windows
from .provenance import content_hash


def _orders_values(partition: str) -> dict:
    windows = upstream_windows(config.ORDERS_WINDOW, partition)
    raw_partitions = windows.get(config.RAW_ORDERS, [])
    records = []
    for raw_partition in raw_partitions:
        seq = lake.latest_seq(config.RAW_ORDERS, raw_partition)
        if seq is None:
            raise KeyError(f"baseline cannot compute: raw_orders/{raw_partition} missing")
        records.append(lake.read_revision(config.RAW_ORDERS, raw_partition, seq))
    gross = round(sum(record["values"]["gross_amount"] for record in records), 2)
    demand = round(sum(record["values"]["order_count"] * 10.0 for record in records), 2)
    return {
        "window_partitions": raw_partitions,
        "rolling_amount": gross,
        "demand_index": demand,
    }


def _windows_fingerprint(partition: str) -> str:
    """The input fingerprint a correct orders_window result would carry."""
    windows = upstream_windows(config.ORDERS_WINDOW, partition)
    raw_partitions = windows.get(config.RAW_ORDERS, [])
    entries = []
    for raw_partition in raw_partitions:
        seq = lake.latest_seq(config.RAW_ORDERS, raw_partition)
        if seq is None:
            raise KeyError(f"baseline cannot compute: raw_orders/{raw_partition} missing")
        entries.append(
            {
                "asset": config.RAW_ORDERS,
                "partition": raw_partition,
                "version": f"seq-{seq}",
            }
        )
    return lake.fingerprint_inputs(entries)


def _settlement_values(partition: str, orders_baseline: dict) -> dict:
    fx_seq = lake.latest_seq(config.RAW_FX, partition)
    if fx_seq is None:
        raise KeyError(f"baseline cannot compute: raw_fx/{partition} missing")
    fx_rate = lake.read_revision(config.RAW_FX, partition, fx_seq)["values"]["usd_per_unit"]
    settled = round(orders_baseline["rolling_amount"] * fx_rate, 2)
    return {
        "rolling_amount": orders_baseline["rolling_amount"],
        "usd_per_unit": fx_rate,
        "settled_usd": settled,
        "window_input_fingerprint": _windows_fingerprint(partition),
    }


def baseline_for(asset: str, partition: str, partition_set: list[str]) -> dict:
    """Recompute one derived partition straight from source revisions."""
    if asset == config.ORDERS_WINDOW:
        values = _orders_values(partition)
        return {"values": values, "values_hash": content_hash(values)}
    if asset == config.SETTLEMENT:
        orders_baseline = _orders_values(partition)
        values = _settlement_values(partition, orders_baseline)
        return {"values": values, "values_hash": content_hash(values)}
    raise KeyError(f"{asset} has no baseline computation")


def _claimed_versions(asset: str, partition: str, published: dict) -> dict:
    """Which upstream versions the published result says it consumed."""
    return {
        (item["asset"], item["partition"]): item.get("version")
        for item in published.get("inputs", [])
    }


def _expected_versions(asset: str, partition: str) -> dict:
    """The versions a correct result would read *today*."""
    expected = {}
    windows = upstream_windows(asset, partition)
    for dep_asset, dep_partitions in windows.items():
        if dep_asset in config.SOURCE_ASSETS:
            for dep_partition in dep_partitions:
                seq = lake.latest_seq(dep_asset, dep_partition)
                expected[(dep_asset, dep_partition)] = None if seq is None else f"seq-{seq}"
        elif dep_asset == config.ORDERS_WINDOW and asset == config.SETTLEMENT:
            for dep_partition in dep_partitions:
                published = lake.read_published(config.ORDERS_WINDOW, dep_partition)
                expected[(dep_asset, dep_partition)] = (
                    published.get("values_hash") if published else None
                )
    return expected


def compare_partition(asset: str, partition: str, partition_set: list[str]) -> dict:
    published = lake.read_published(asset, partition)
    item = {"asset": asset, "partition": partition}

    if published is None:
        return {**item, "status": "missing_result", "detail": "no published file"}

    if not published.get("committed") or published.get("values") is None:
        return {
            **item,
            "status": "untrusted_file",
            "detail": "published file present but no commit marker or values (interrupted publish)",
        }

    try:
        baseline = baseline_for(asset, partition, partition_set)
    except KeyError as exc:
        return {**item, "status": "baseline_unavailable", "detail": str(exc)}

    value_match = published["values"] == baseline["values"]
    hash_match = published.get("values_hash") == baseline["values_hash"]

    stale = []
    expected = _expected_versions(asset, partition)
    claimed = _claimed_versions(asset, partition, published)
    for key, expected_version in expected.items():
        claimed_version = claimed.get(key)
        if claimed_version != expected_version:
            stale.append(
                {
                    "input": f"{key[0]}/{key[1]}",
                    "claimed_version": claimed_version,
                    "latest_version": expected_version,
                }
            )
    # A result that omits an input it should have read is also stale.
    for key in expected:
        if key not in claimed:
            if not any(entry["input"] == f"{key[0]}/{key[1]}" for entry in stale):
                stale.append(
                    {
                        "input": f"{key[0]}/{key[1]}",
                        "claimed_version": None,
                        "latest_version": expected[key],
                    }
                )

    status = "match"
    detail = ""
    if stale:
        status = "stale_input"
        detail = "result values may match but the recorded inputs are not the latest revisions"
    if not (value_match and hash_match):
        status = "value_mismatch"
        detail = "published values differ from an independent recomputation"

    return {
        **item,
        "status": status,
        "detail": detail,
        "value_match": value_match,
        "hash_match": hash_match,
        "stale_inputs": stale,
        "published_values": published.get("values"),
        "baseline_values": baseline["values"],
    }


def verify(partition_set: list[str] | None = None) -> dict:
    partition_set = partition_set or config.DEFAULT_PARTITIONS
    rows = [
        compare_partition(asset, partition, partition_set)
        for asset in config.DERIVED_ASSETS
        for partition in partition_set
    ]

    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row["status"]] = by_status.get(row["status"], 0) + 1

    findings = [row for row in rows if row["status"] not in ("match", "missing_result")]
    passed = not findings and by_status.get("missing_result", 0) == 0
    return {
        "schema": "verification_report/1",
        "partition_set": partition_set,
        "totals": by_status,
        "passed": passed,
        "findings": findings,
        "rows": rows,
        "rule": (
            "each result is compared against an independent recomputation from source revisions; "
            "both values and the recorded input revisions must agree"
        ),
    }
