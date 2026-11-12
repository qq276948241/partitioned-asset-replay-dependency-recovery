"""Local lake of source revisions plus the staging/publish protocol for derived assets.

Layout under the workspace:

``lake/<asset>/<partition>/rev-<seq>.json``
    Immutable source revisions. A correction appends a new ``seq``; older
    revisions stay readable so stale reads can be detected after the fact.
``staging/<asset>/<partition>/<attempt>/``
    Work in progress for one execution attempt. Files appear as work proceeds,
    so an interrupted attempt leaves genuine partial material behind.
``published/<asset>/<partition>.json``
    A committed derived result. Written atomically, only after the attempt
    reached its final step.
"""

import shutil
from pathlib import Path

from . import config
from .provenance import content_hash, read_json, sha256_file, write_json_atomic

REV_PREFIX = "rev-"
INPUT_MANIFEST = "input_manifest.json"
VALUES = "values.json"
COMMITTED = "COMMITTED"


def asset_dir(asset: str) -> Path:
    return config.lake_dir() / asset


def revision_dir(asset: str, partition: str) -> Path:
    return asset_dir(asset) / partition


def revision_path(asset: str, partition: str, seq: int) -> Path:
    return revision_dir(asset, partition) / f"{REV_PREFIX}{seq}.json"


def list_revisions(asset: str, partition: str) -> list[int]:
    directory = revision_dir(asset, partition)
    if not directory.is_dir():
        return []
    seqs = []
    for path in directory.iterdir():
        if path.name.startswith(REV_PREFIX) and path.suffix == ".json":
            stem = path.stem[len(REV_PREFIX) :]
            if stem.isdigit():
                seqs.append(int(stem))
    return sorted(seqs)


def latest_seq(asset: str, partition: str) -> int | None:
    seqs = list_revisions(asset, partition)
    return seqs[-1] if seqs else None


def read_revision(asset: str, partition: str, seq: int) -> dict:
    return read_json(revision_path(asset, partition, seq))


def write_revision(asset: str, partition: str, seq: int, payload: dict) -> Path:
    path = revision_path(asset, partition, seq)
    write_json_atomic(path, payload)
    return path


def revision_summary(asset: str, partition: str, seq: int) -> dict:
    """Stable identity of one revision, safe to embed in run metadata."""
    path = revision_path(asset, partition, seq)
    return {
        "asset": asset,
        "partition": partition,
        "seq": seq,
        "content_hash": sha256_file(path),
    }


def present_partitions(asset: str, partitions: list[str]) -> set[str]:
    return {p for p in partitions if latest_seq(asset, p) is not None}


# ---------------------------------------------------------------------------
# Staging attempts
# ---------------------------------------------------------------------------


def attempt_dir(asset: str, partition: str, attempt_id: str) -> Path:
    return config.staging_dir() / asset / partition / attempt_id


def stage_input_manifest(asset: str, partition: str, attempt_id: str, manifest: dict) -> Path:
    path = attempt_dir(asset, partition, attempt_id) / INPUT_MANIFEST
    write_json_atomic(path, manifest)
    return path


def stage_values(asset: str, partition: str, attempt_id: str, values: dict) -> Path:
    path = attempt_dir(asset, partition, attempt_id) / VALUES
    write_json_atomic(path, values)
    return path


def stage_commit(asset: str, partition: str, attempt_id: str) -> Path:
    path = attempt_dir(asset, partition, attempt_id) / COMMITTED
    write_json_atomic(path, {"committed": True})
    return path


def attempt_state(asset: str, partition: str, attempt_id: str) -> dict:
    directory = attempt_dir(asset, partition, attempt_id)
    return {
        "asset": asset,
        "partition": partition,
        "attempt_id": attempt_id,
        "has_input_manifest": (directory / INPUT_MANIFEST).exists(),
        "has_values": (directory / VALUES).exists(),
        "committed": (directory / COMMITTED).exists(),
    }


# ---------------------------------------------------------------------------
# Published results
# ---------------------------------------------------------------------------


def published_path(asset: str, partition: str) -> Path:
    return config.published_dir() / asset / f"{partition}.json"


def read_published(asset: str, partition: str) -> dict | None:
    path = published_path(asset, partition)
    if not path.exists():
        return None
    return read_json(path)


def published_partitions(asset: str, partitions: list[str]) -> set[str]:
    return {p for p in partitions if published_path(asset, p).exists()}


def publish_result(asset: str, partition: str, payload: dict) -> Path:
    path = published_path(asset, partition)
    write_json_atomic(path, payload)
    return path


# ---------------------------------------------------------------------------
# Reconciliation: what exists vs what is trustworthy
# ---------------------------------------------------------------------------


def scan_attempts() -> list[dict]:
    """Every staging attempt on disk, regardless of completion."""
    rows = []
    root = config.staging_dir()
    if not root.is_dir():
        return rows
    for asset_path in sorted(root.iterdir()):
        if not asset_path.is_dir():
            continue
        for partition_path in sorted(asset_path.iterdir()):
            if not partition_path.is_dir():
                continue
            rows.extend(
                attempt_state(asset_path.name, partition_path.name, attempt_path.name)
                for attempt_path in sorted(partition_path.iterdir())
                if attempt_path.is_dir()
            )
    return rows


def quarantine_partial_attempts() -> list[dict]:
    """Move uncommitted attempts aside so they can never be chosen as input.

    An uncommitted attempt is genuine half-finished material: the process died
    between reading inputs and committing a result. It is kept for inspection
    but leaves the staging area, which is what input selection consults.
    """
    moved = []
    for row in scan_attempts():
        if row["committed"]:
            continue
        source = attempt_dir(row["asset"], row["partition"], row["attempt_id"])
        target = config.quarantine_dir() / row["asset"] / row["partition"] / row["attempt_id"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))
        moved.append({**row, "quarantined_to": str(target)})
    return moved


def trustworthy_partitions(asset: str, partitions: list[str]) -> set[str]:
    """Partitions with a committed published result.

    File presence alone is not trust: a publish interrupted mid-way can leave a
    file whose run never recorded success. Such a file is reported separately so
    verification can flag it instead of silently consuming it.
    """
    trusted = set()
    for partition in partitions:
        payload = read_published(asset, partition)
        if payload and payload.get("committed") and payload.get("values") is not None:
            trusted.add(partition)
    return trusted


def untrusted_published_files(asset: str, partitions: list[str]) -> list[str]:
    """Published files that exist but are not committed results."""
    dubious = []
    for partition in partitions:
        path = published_path(asset, partition)
        if not path.exists():
            continue
        payload = read_json(path)
        if not payload.get("committed") or payload.get("values") is None:
            dubious.append(partition)
    return dubious


def fingerprint_inputs(manifest: list[dict]) -> str:
    """Hash of which upstream versions a result consumed.

    Source revisions identify themselves by ``seq``; derived results identify
    themselves by the ``values_hash`` of the output they produced. Either way the
    manifest entry carries one ``version`` string, so both kinds of input are
    comparable in one fingerprint.
    """
    return content_hash(
        sorted(
            (
                {
                    "asset": entry["asset"],
                    "partition": entry["partition"],
                    "version": entry["version"],
                }
                for entry in manifest
            ),
            key=lambda item: (item["asset"], item["partition"]),
        )
    )
