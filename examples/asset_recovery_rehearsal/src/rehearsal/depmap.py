"""Build a checkable dependency map and enumerate the partitions an event affects.

The map is derived from the same asset definitions that execution uses, so it
cannot drift from the graph. It spells out three rules the drill exercises:

``start_boundary``
    A trailing window is clamped by the upstream partition definition; early
    partitions pull fewer than the nominal window size.
``missing_input``
    A downstream partition is computable only when every partition in its
    resolved window has a source revision; otherwise it is skipped, never
    computed on partial inputs.
``correction``
    A new source revision affects the partition itself and every downstream
    partition whose window covers it, for every downstream asset in turn.
"""

from pathlib import Path

import dagster as dg

from . import config
from .assets import ALL_ASSETS, daily
from .provenance import write_json_atomic


def _asset_defs() -> dict[str, dg.AssetsDefinition]:
    return {a.key.to_user_string(): a for a in ALL_ASSETS}


def upstream_windows(asset: str, partition: str) -> dict[str, list[str]]:
    """Resolve each declared dependency to its actual upstream partition set."""
    asset_def = _asset_defs()[asset]
    subset = daily.empty_subset().with_partition_keys([partition])
    resolved: dict[str, list[str]] = {}
    for dep in asset_def.get_asset_spec().deps:
        upstream_name = dep.asset_key.to_user_string()
        if upstream_name not in _asset_defs():
            continue
        result = dep.partition_mapping.get_upstream_mapped_partitions_result_for_partitions(
            subset, daily, daily
        )
        resolved[upstream_name] = result.partitions_subset.get_partition_keys()
    return resolved


def downstream_impact(upstream_asset: str, upstream_partition: str) -> list[dict]:
    """Every (asset, partition) whose resolved inputs include this source partition.

    This inverts the declared window mapping through Dagster's own
    ``get_downstream_partitions_for_partitions`` rather than a hand-rolled offset
    rule, so start-boundary clamping is inherited from the framework.
    """
    impacts = []
    for asset in config.DERIVED_ASSETS:
        for partition in config.DEFAULT_PARTITIONS:
            windows = upstream_windows(asset, partition)
            for dep_asset, partitions_in_window in windows.items():
                if dep_asset == upstream_asset and upstream_partition in partitions_in_window:
                    impacts.append(
                        {
                            "asset": asset,
                            "partition": partition,
                            "via_dependency": dep_asset,
                            "window": partitions_in_window,
                        }
                    )
    return impacts


def transitive_impact(changed: list[dict]) -> list[dict]:
    """Close over downstream assets: a recomputed window also moves settlement."""
    seen = {(item["asset"], item["partition"]) for item in changed}
    frontier = list(seen)
    result = list(changed)
    while frontier:
        asset, partition = frontier.pop()
        for downstream in config.DERIVED_ASSETS:
            if downstream == asset:
                continue
            windows = upstream_windows(downstream, partition)
            if asset in windows:
                key = (downstream, partition)
                if key not in seen:
                    seen.add(key)
                    frontier.append(key)
                    result.append(
                        {
                            "asset": downstream,
                            "partition": partition,
                            "via_dependency": asset,
                            "window": windows.get(asset, []),
                        }
                    )
    return result


def _mapping_description(mapping: object) -> dict:
    return {
        "type": type(mapping).__name__,
        "start_offset": getattr(mapping, "start_offset", None),
        "end_offset": getattr(mapping, "end_offset", None),
    }


def build_dependency_map(partitions: list[str]) -> dict:
    edges = []
    for asset in config.DERIVED_ASSETS:
        asset_def = _asset_defs()[asset]
        edges.extend(
            {
                "downstream": asset,
                "upstream": dep.asset_key.to_user_string(),
                "mapping": _mapping_description(dep.partition_mapping),
            }
            for dep in asset_def.get_asset_spec().deps
        )

    windows = {
        f"{asset}/{partition}": upstream_windows(asset, partition)
        for asset in config.DERIVED_ASSETS
        for partition in partitions
    }

    clamped = []
    nominal = abs(config.WINDOW_OFFSET_START) + abs(config.WINDOW_OFFSET_END) + 1
    for key, resolved in windows.items():
        asset, partition = key.split("/", 1)
        if asset != config.ORDERS_WINDOW:
            continue
        for dep_asset, window in resolved.items():
            if dep_asset != config.RAW_ORDERS:
                continue
            if len(window) < nominal:
                clamped.append(
                    {"partition": partition, "resolved_window": window, "nominal_size": nominal}
                )

    return {
        "schema": "dependency_map/1",
        "window": {"start": partitions[0], "end": partitions[-1], "count": len(partitions)},
        "assets": list(config.ALL_ASSETS),
        "edges": edges,
        "per_partition_windows": windows,
        "start_boundary_clamped": clamped,
        "rules": {
            "start_boundary": (
                "window start is clamped to the first partition of the upstream definition; "
                "partitions listed in start_boundary_clamped pull fewer than the nominal window"
            ),
            "missing_input": (
                "a downstream partition runs only when every partition in its resolved window "
                "has a source revision; otherwise it is reported not_computable, never computed"
            ),
            "correction": (
                "a new source revision supersedes the old seq and moves the partition itself plus "
                "every downstream partition whose resolved window covers it, transitively"
            ),
        },
    }


def write_dependency_map(partitions: list[str], path: Path | None = None) -> Path:
    target = path or (config.reports_dir() / "dependency_map.json")
    write_json_atomic(target, build_dependency_map(partitions))
    return target
