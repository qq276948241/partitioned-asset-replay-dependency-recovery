"""Local-only rehearsal of dependent backfills for a date-partitioned asset graph.

The package contains the Dagster asset definitions used for execution, a planner that
decides which downstream partitions are genuinely computable, a runner that records
every decision next to the real Dagster run, and a verifier that compares results
against a full rebuild of the same inputs.
"""
