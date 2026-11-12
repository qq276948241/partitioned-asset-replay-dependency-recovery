"""Plan which downstream partitions can actually run, and why the others cannot.

Evaluation walks the asset graph in dependency order. A derived input that is
missing from the lake but *is scheduled earlier in the same plan* counts as
satisfiable, so a single plan can express the whole chain. Anything still
missing is reported with the exact inputs that block it.
"""

from . import config, lake
from .depmap import upstream_windows
from .provenance import stable_id, write_json_atomic


def _source_revision(asset: str, partition: str) -> dict | None:
    seq = lake.latest_seq(asset, partition)
    if seq is None:
        return None
    return lake.revision_summary(asset, partition, seq)


def _already_current(asset: str, partition: str, resolved_inputs: list[dict]) -> bool:
    """A committed result whose recorded input fingerprint matches today's inputs.

    Re-running it would republish byte-identical values. Skipping it is what makes
    a repeated request idempotent rather than a duplicate publish.
    """
    published = lake.read_published(asset, partition)
    if not published or not published.get("committed"):
        return False
    expected = lake.fingerprint_inputs(
        [
            {"asset": item["asset"], "partition": item["partition"], "version": _version_for(item)}
            for item in resolved_inputs
        ]
    )
    return published.get("input_fingerprint") == expected


def _version_for(item: dict) -> str | None:
    if item.get("committed"):
        payload = lake.read_published(item["asset"], item["partition"]) or {}
        return payload.get("values_hash")
    if item.get("scheduled"):
        return f"scheduled:{item['asset']}/{item['partition']}"
    seq = item.get("seq")
    return None if seq is None else f"seq-{seq}"


def evaluate_partition(
    asset: str, partition: str, planned: dict[tuple[str, str], str] | None = None
) -> dict:
    """Evaluate one partition.

    ``planned`` maps already-scheduled upstream items to their status; it lets a
    downstream partition depend on work scheduled earlier in the same plan.
    """
    planned = planned or {}
    windows = upstream_windows(asset, partition)
    resolved_inputs = []
    missing = []
    for dep_asset, dep_partitions in windows.items():
        if dep_asset in config.SOURCE_ASSETS:
            for dep_partition in dep_partitions:
                summary = _source_revision(dep_asset, dep_partition)
                if summary is None:
                    missing.append({"asset": dep_asset, "partition": dep_partition})
                else:
                    resolved_inputs.append(summary)
        elif dep_asset in config.DERIVED_ASSETS:
            committed = lake.trustworthy_partitions(dep_asset, dep_partitions)
            for dep_partition in dep_partitions:
                # The plan wins over the lake: if this upstream partition is already
                # scheduled to be recomputed, a downstream result that reads the
                # currently-published value is stale by construction.
                if planned.get((dep_asset, dep_partition)) == "computable":
                    resolved_inputs.append(
                        {"asset": dep_asset, "partition": dep_partition, "scheduled": True}
                    )
                elif dep_partition in committed:
                    resolved_inputs.append(
                        {"asset": dep_asset, "partition": dep_partition, "committed": True}
                    )
                else:
                    missing.append({"asset": dep_asset, "partition": dep_partition})

    if missing:
        status = "not_computable"
    elif _already_current(asset, partition, resolved_inputs) and not any(
        item.get("scheduled") for item in resolved_inputs
    ):
        status = "already_current"
    else:
        status = "computable"

    return {
        "asset": asset,
        "partition": partition,
        "status": status,
        "missing_inputs": missing,
        "resolved_inputs": resolved_inputs,
        "window": windows,
    }


def _evaluate_in_order(
    targets: list[tuple[str, str]], planned: dict[tuple[str, str], str] | None = None
) -> list[dict]:
    """Evaluate targets in asset dependency order, feeding scheduled work forward."""
    planned = dict(planned or {})
    entries = []
    ordered_assets = list(config.DERIVED_ASSETS)
    for asset in ordered_assets:
        for partition in [p for a, p in targets if a == asset]:
            entry = evaluate_partition(asset, partition, planned)
            planned[(asset, partition)] = entry["status"]
            entries.append(entry)
    return entries


def build_plan(reason: str, partitions: list[str] | None = None) -> dict:
    partitions = partitions or config.DEFAULT_PARTITIONS
    targets = [(asset, partition) for asset in config.DERIVED_ASSETS for partition in partitions]
    entries = _evaluate_in_order(targets)

    computable = [e for e in entries if e["status"] == "computable"]
    already = [e for e in entries if e["status"] == "already_current"]
    blocked = [e for e in entries if e["status"] == "not_computable"]
    plan = {
        "schema": "execution_plan/1",
        "reason": reason,
        "partitions": partitions,
        "totals": {
            "evaluated": len(entries),
            "computable": len(computable),
            "already_current": len(already),
            "not_computable": len(blocked),
        },
        "computable": computable,
        "already_current": already,
        "not_computable": blocked,
        "notes": (
            "not_computable partitions are excluded deliberately; running them on partial "
            "inputs would publish results whose provenance cannot be defended. already_current "
            "partitions hold committed results whose input fingerprint still matches, so re-running "
            "them would only republish identical values"
        ),
    }
    plan["plan_id"] = stable_id(
        "plan",
        {"reason": reason, "computable": [f"{e['asset']}/{e['partition']}" for e in computable]},
    )
    return plan


def write_plan(plan: dict, name: str) -> str:
    path = config.plans_dir() / f"{name}.json"
    write_json_atomic(path, plan)
    return str(path)


def affected_scope(changed_sources: list[dict], partitions: list[str] | None = None) -> dict:
    """Scope a correction: which partitions genuinely change, and nothing more."""
    from .depmap import transitive_impact

    direct = []
    for item in changed_sources:
        direct.extend(
            {
                "asset": impact["asset"],
                "partition": impact["partition"],
                "via_dependency": impact["via_dependency"],
            }
            for impact in _impacts_for(item["asset"], item["partition"], partitions)
        )
    scope = transitive_impact(direct) if direct else []
    unique = {}
    for item in scope:
        unique[(item["asset"], item["partition"])] = {
            "asset": item["asset"],
            "partition": item["partition"],
            "via_dependency": item.get("via_dependency"),
        }
    ordered = [unique[key] for key in sorted(unique)]
    return {
        "schema": "affected_scope/1",
        "changed_sources": changed_sources,
        "reason": "correction",
        "affected": ordered,
        "unaffected_partitions": sorted(
            set(partitions or config.DEFAULT_PARTITIONS) - {item["partition"] for item in ordered}
        ),
        "rule": (
            "a source revision affects the partitions of every downstream asset whose resolved "
            "window includes it; results outside this set are reused, not recomputed"
        ),
    }


def _impacts_for(asset: str, partition: str, partitions: list[str] | None) -> list[dict]:
    from .depmap import downstream_impact

    impacts = downstream_impact(asset, partition)
    if partitions is not None:
        impacts = [i for i in impacts if i["partition"] in set(partitions)]
    return impacts


def scope_plan(reason: str, scope: dict, partitions: list[str] | None = None) -> dict:
    """Plan restricted to an affected scope, still refusing uncomputable partitions."""
    targets = sorted({(item["asset"], item["partition"]) for item in scope["affected"]})
    entries = _evaluate_in_order(targets)
    computable = [e for e in entries if e["status"] == "computable"]
    already = [e for e in entries if e["status"] == "already_current"]
    blocked = [e for e in entries if e["status"] == "not_computable"]
    plan = {
        "schema": "execution_plan/1",
        "reason": reason,
        "scope": {
            "affected": scope["affected"],
            "unaffected_partitions": scope["unaffected_partitions"],
        },
        "totals": {
            "evaluated": len(entries),
            "computable": len(computable),
            "already_current": len(already),
            "not_computable": len(blocked),
        },
        "computable": computable,
        "already_current": already,
        "not_computable": blocked,
        "notes": (
            "only partitions inside the affected scope are considered; unaffected published results "
            "are reused rather than recomputed"
        ),
    }
    plan["plan_id"] = stable_id(
        "plan",
        {"reason": reason, "computable": [f"{e['asset']}/{e['partition']}" for e in computable]},
    )
    return plan
