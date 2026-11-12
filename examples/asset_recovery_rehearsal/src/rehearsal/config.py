"""Configuration and filesystem layout for the backfill rehearsal."""

import dataclasses
import datetime
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# A fixed, historical window keeps every run reproducible. Twelve days gives the
# seven-day window room to clamp at the start boundary and later recover.
PARTITION_START = "2026-10-01"
PARTITION_END_EXCLUSIVE = "2026-10-13"
DEFAULT_PARTITIONS = [
    (datetime.date(2026, 10, 1) + datetime.timedelta(days=offset)).isoformat()
    for offset in range(12)
]

WINDOW_OFFSET_START = -6
WINDOW_OFFSET_END = 0

RAW_ORDERS = "raw_orders"
RAW_FX = "raw_fx"
ORDERS_WINDOW = "orders_window_7d"
SETTLEMENT = "daily_settlement"

SOURCE_ASSETS = (RAW_ORDERS, RAW_FX)
DERIVED_ASSETS = (ORDERS_WINDOW, SETTLEMENT)
ALL_ASSETS = (RAW_ORDERS, RAW_FX, ORDERS_WINDOW, SETTLEMENT)

# Deliberate gaps in the first sample-data generation. These are the "upstream
# partitions that never arrived" the drill is built around.
INITIAL_MISSING_ORDERS = ("2026-10-04",)
INITIAL_MISSING_FX = ("2026-10-07",)

# The partition corrected after the first recovery pass.
CORRECTION_TARGET = "2026-10-02"

ENV_WORKSPACE = "REHEARSAL_WORKSPACE"
ENV_FAULT_STAGE = "REHEARSAL_FAULT_STAGE"
ENV_FAULT_ACTION = "REHEARSAL_FAULT_ACTION"
ENV_FAULT_ASSET = "REHEARSAL_FAULT_ASSET"
ENV_FAULT_PARTITION = "REHEARSAL_FAULT_PARTITION"
ENV_FAULT_TOKEN = "REHEARSAL_FAULT_TOKEN"

FAULT_STAGES = ("read", "write", "publish")


def workspace_root() -> Path:
    configured = os.getenv(ENV_WORKSPACE)
    if configured:
        return Path(configured).expanduser().resolve()
    return PROJECT_ROOT / ".rehearsal"


def dagster_home() -> Path:
    return workspace_root() / "dagster_home"


def lake_dir() -> Path:
    return workspace_root() / "lake"


def published_dir() -> Path:
    return workspace_root() / "published"


def staging_dir() -> Path:
    return workspace_root() / "staging"


def quarantine_dir() -> Path:
    return workspace_root() / "quarantine"


def journal_dir() -> Path:
    return workspace_root() / "journal"


def state_dir() -> Path:
    return workspace_root() / "state"


def plans_dir() -> Path:
    return state_dir() / "plans"


def reports_dir() -> Path:
    return workspace_root() / "reports"


def code_version() -> str:
    return "rehearsal-code-v1"


def all_directories() -> list[Path]:
    return [
        workspace_root(),
        dagster_home(),
        lake_dir(),
        published_dir(),
        staging_dir(),
        quarantine_dir(),
        journal_dir(),
        state_dir(),
        plans_dir(),
        reports_dir(),
    ]


@dataclasses.dataclass(frozen=True)
class Fault:
    """A single controllable interruption requested for one work item."""

    action: str
    stage: str
    asset: str
    partition: str
    token: str


def read_fault() -> Fault | None:
    stage = os.getenv(ENV_FAULT_STAGE)
    if not stage:
        return None
    return Fault(
        action=os.getenv(ENV_FAULT_ACTION, "interrupt"),
        stage=stage,
        asset=os.getenv(ENV_FAULT_ASSET, ""),
        partition=os.getenv(ENV_FAULT_PARTITION, ""),
        token=os.getenv(ENV_FAULT_TOKEN, ""),
    )


def fault_applies(fault: Fault | None, asset: str, partition: str) -> bool:
    if fault is None:
        return False
    if fault.stage not in FAULT_STAGES:
        return False
    return fault.asset == asset and fault.partition == partition
