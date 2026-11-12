"""Generate traceable sample data: dated source partitions plus deliberate gaps.

Every revision records which upstream state produced it. Re-running generation
for an already-present revision is a no-op, so the same request issued twice
cannot invent a new version.
"""

import random

from . import config
from .lake import latest_seq, list_revisions, revision_summary, write_revision
from .provenance import content_hash, utc_now

ORDERS_REVISION = "orders-src-v1"
FX_REVISION = "fx-src-v1"


def _seed(asset: str, partition: str, salt: str) -> random.Random:
    return random.Random(
        int(content_hash({"asset": asset, "partition": partition, "salt": salt})[:8], 16)
    )


def make_orders(partition: str, salt: str) -> dict:
    rng = _seed(config.RAW_ORDERS, partition, salt)
    line_items = rng.randint(20, 40)
    amounts = [round(rng.uniform(5.0, 400.0), 2) for _ in range(line_items)]
    return {
        "asset": config.RAW_ORDERS,
        "partition": partition,
        "revision_label": ORDERS_REVISION,
        "salt": salt,
        "values": {
            "order_count": line_items,
            "gross_amount": round(sum(amounts), 2),
            "largest_order": round(max(amounts), 2),
        },
    }


def make_fx(partition: str, salt: str) -> dict:
    rng = _seed(config.RAW_FX, partition, salt)
    rate = round(rng.uniform(0.84, 0.92), 5)
    return {
        "asset": config.RAW_FX,
        "partition": partition,
        "revision_label": FX_REVISION,
        "salt": salt,
        "values": {"usd_per_unit": rate},
    }


def _write_source(
    asset: str,
    partition: str,
    payload: dict,
    revision_source: str,
    supersedes: int | None,
) -> dict:
    seq = (latest_seq(asset, partition) or 0) + 1
    record = {
        "schema": "source_revision/1",
        "asset": asset,
        "partition": partition,
        "seq": seq,
        "created_at": utc_now(),
        "revision_label": payload["revision_label"],
        "revision_source": revision_source,
        "supersedes": supersedes,
        "values": payload["values"],
        "payload_hash": content_hash(payload["values"]),
    }
    path = write_revision(asset, partition, seq, record)
    return {**revision_summary(asset, partition, seq), "path": str(path), "supersedes": supersedes}


def generate_initial(
    partitions: list[str], missing_orders: tuple[str, ...], missing_fx: tuple[str, ...]
) -> dict:
    """Create the first sample generation, withholding the scheduled gaps."""
    written = []
    skipped = []
    gaps = []
    for partition in partitions:
        for asset, payload_fn, missing in (
            (config.RAW_ORDERS, make_orders, missing_orders),
            (config.RAW_FX, make_fx, missing_fx),
        ):
            if partition in missing:
                gaps.append(
                    {"asset": asset, "partition": partition, "reason": "not delivered upstream"}
                )
                continue
            if latest_seq(asset, partition) is not None:
                skipped.append(
                    {"asset": asset, "partition": partition, "reason": "already present"}
                )
                continue
            payload = payload_fn(partition, "initial")
            written.append(
                _write_source(asset, partition, payload, "initial_generation", supersedes=None)
            )
    return {"written": written, "skipped": skipped, "gaps": gaps}


def restore_gaps(
    partitions: list[str], missing_orders: tuple[str, ...], missing_fx: tuple[str, ...]
) -> dict:
    """Backfill only the withheld partitions, as late-arriving upstream data."""
    written = []
    skipped = []
    for partition in partitions:
        for asset, payload_fn, missing in (
            (config.RAW_ORDERS, make_orders, missing_orders),
            (config.RAW_FX, make_fx, missing_fx),
        ):
            if partition not in missing:
                continue
            if latest_seq(asset, partition) is not None:
                skipped.append(
                    {"asset": asset, "partition": partition, "reason": "gap already filled"}
                )
                continue
            payload = payload_fn(partition, "late_arrival")
            written.append(
                _write_source(asset, partition, payload, "late_upstream_arrival", supersedes=None)
            )
    return {"written": written, "skipped": skipped}


def correct_partition(partitions: list[str], asset: str, partition: str, salt: str) -> dict:
    """Append a corrected revision for one source partition."""
    if partition not in partitions:
        raise ValueError(f"partition {partition} is outside the rehearsal window")
    previous = latest_seq(asset, partition)
    if previous is None:
        raise ValueError(f"cannot correct {asset}/{partition}: no revision exists")
    payload_fn = make_orders if asset == config.RAW_ORDERS else make_fx
    payload = payload_fn(partition, salt)
    record = _write_source(asset, partition, payload, f"correction:{salt}", supersedes=previous)
    return {
        **record,
        "previous_seq": previous,
        "values_before": read_values(asset, partition, previous),
    }


def read_values(asset: str, partition: str, seq: int) -> dict:
    from .lake import read_revision

    return read_revision(asset, partition, seq)["values"]


def source_inventory(partitions: list[str]) -> list[dict]:
    """Every source revision on disk, for the traceability report."""
    rows = []
    for asset in config.SOURCE_ASSETS:
        for partition in partitions:
            rows.extend(
                revision_summary(asset, partition, seq) for seq in list_revisions(asset, partition)
            )
    return rows
