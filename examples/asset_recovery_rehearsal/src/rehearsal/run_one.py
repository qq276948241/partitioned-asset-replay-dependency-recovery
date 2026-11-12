"""Child-process entry point: execute exactly one asset partition as one Dagster run.

Run from the parent with ``REHEARSAL_*`` environment variables set. The run id is
supplied by the parent so both processes agree on which run to inspect.
"""

import json
import os
import sys

import dagster as dg

from rehearsal.assets import ALL_ASSETS


def _selected_asset(asset_name: str) -> dg.AssetsDefinition:
    for asset_def in ALL_ASSETS:
        if asset_def.key.to_user_string() == asset_name:
            return asset_def
    raise SystemExit(f"unknown asset {asset_name}")


def main() -> int:
    asset = os.environ["REHEARSAL_ASSET"]
    partition = os.environ["REHEARSAL_PARTITION"]
    tags = json.loads(os.environ.get("REHEARSAL_RUN_TAGS", "{}"))

    instance = dg.DagsterInstance.get()
    selected = _selected_asset(asset)

    try:
        result = dg.materialize(
            [selected],
            instance=instance,
            partition_key=partition,
            selection=[selected],
            tags=tags,
            raise_on_error=False,
        )
    except Exception as exc:
        print(json.dumps({"success": False, "error": repr(exc)}))
        return 3

    print(json.dumps({"success": result.success, "run_id": result.run_id}))
    return 0 if result.success else 2


if __name__ == "__main__":
    sys.exit(main())
