"""Asset definitions. Dependency edges and partition mappings live here and only here.

Derived assets read *committed* published results or source revisions; they never
rely on Dagster's implicit ordering for correctness. Dependency edges are declared
so the orchestration UI shows the real graph and so the dependency map can be
derived from the same declarations that execution uses.
"""

import dagster as dg

from . import config
from .lake import latest_seq, read_revision, revision_summary

daily = dg.DailyPartitionsDefinition(
    start_date=config.PARTITION_START, end_date=config.PARTITION_END_EXCLUSIVE
)

ORDERS_WINDOW_MAPPING = dg.TimeWindowPartitionMapping(
    start_offset=config.WINDOW_OFFSET_START, end_offset=config.WINDOW_OFFSET_END
)


def _ingest(context: dg.AssetExecutionContext, asset_name: str) -> dg.MaterializeResult:
    partition = context.partition_key
    seq = latest_seq(asset_name, partition)
    if seq is None:
        raise dg.Failure(
            description=f"source partition {asset_name}/{partition} has no revision",
            metadata={"asset": asset_name, "partition": partition},
        )
    record = read_revision(asset_name, partition, seq)
    summary = revision_summary(asset_name, partition, seq)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(summary["content_hash"]),
        metadata={
            "rehearsal/role": "ingest",
            "rehearsal/asset": asset_name,
            "rehearsal/partition": partition,
            "rehearsal/revision_seq": seq,
            "rehearsal/revision_label": record["revision_label"],
            "rehearsal/content_hash": summary["content_hash"],
            "rehearsal/revision_source": record["revision_source"],
        },
    )


@dg.asset(name=config.RAW_ORDERS, partitions_def=daily, group_name="sources")
def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _ingest(context, config.RAW_ORDERS)


@dg.asset(name=config.RAW_FX, partitions_def=daily, group_name="sources")
def raw_fx(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return _ingest(context, config.RAW_FX)


# Imported lazily inside the body to keep this module free of a cycle with the
# compute helpers, which in turn import the assets for their partition definitions.
@dg.asset(
    name=config.ORDERS_WINDOW,
    partitions_def=daily,
    group_name="derived",
    deps=[dg.AssetDep(config.RAW_ORDERS, partition_mapping=ORDERS_WINDOW_MAPPING)],
)
def orders_window_7d(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from .computes import compute_orders_window

    return compute_orders_window(context)


@dg.asset(
    name=config.SETTLEMENT,
    partitions_def=daily,
    group_name="derived",
    deps=[
        dg.AssetDep(config.ORDERS_WINDOW, partition_mapping=dg.IdentityPartitionMapping()),
        dg.AssetDep(config.RAW_FX, partition_mapping=dg.IdentityPartitionMapping()),
    ],
)
def daily_settlement(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from .computes import compute_settlement

    return compute_settlement(context)


ALL_ASSETS = [raw_orders, raw_fx, orders_window_7d, daily_settlement]

defs = dg.Definitions(assets=ALL_ASSETS)
