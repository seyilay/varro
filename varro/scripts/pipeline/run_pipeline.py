#!/usr/bin/env python3
"""
Varro Ingestion Pipeline v2 — runner.

Usage:
  cd /home/openclaw/.openclaw/workspace/varro/scripts
  python3 pipeline/run_pipeline.py [--source SOURCE] [--skip-download]

Sources:
  CALGEM       California WellSTAR (live API, ~242K wells)
  NSTA_UK      UKCS offshore wellbores (live API, ~13K wells)
  AUSTRALIA    WA + SA (local files, ~8.3K wells)
  NETHERLANDS  NLOG batches (local files, ~6.7K wells)
  COLOMBIA     ANH campos + pozos (local files, ~1.3K)
  NORWAY       Sodir exploration wellbores (local file, ~300)
  ALL          Run all of the above in sequence

Examples:
  python3 pipeline/run_pipeline.py --source CALGEM --skip-download
  python3 pipeline/run_pipeline.py --source AUSTRALIA
  python3 pipeline/run_pipeline.py --source ALL --skip-download
"""

import sys
import os
import argparse
import time

# Ensure pipeline package is importable when run from scripts/
sys.path.insert(0, os.path.dirname(__file__))

REGISTRY: dict[str, str] = {
    # Live API sources (download() is a no-op)
    "CALGEM":      "calgem_ingestor.CalGEMIngestor",        # ~242K CA wells
    "NSTA_UK":     "nsta_uk_ingestor.NSTAUKIngestor",       # ~13K UKCS wells
    # Local file sources (download() validates files exist)
    "AUSTRALIA":   "australia_ingestor.AustraliaIngestor",  # ~8.3K WA+SA wells
    "NETHERLANDS": "netherlands_ingestor.NetherlandsIngestor",  # ~6.7K wells
    "COLOMBIA":    "colombia_ingestor.ColombiaIngestor",    # ~1.3K fields+wells
    "NORWAY":      "norway_ingestor.NorwayIngestor",        # ~300 exploration wellbores
}


def _load_ingestor(key: str):
    """Dynamically import and instantiate ingestor class by registry key."""
    module_name, class_name = REGISTRY[key].rsplit(".", 1)
    import importlib
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name)
    return cls()


def run_source(key: str, skip_download: bool = False) -> int:
    ingestor = _load_ingestor(key)
    return ingestor.run(skip_download=skip_download)


def run_all(skip_download: bool = False) -> dict[str, int]:
    results = {}
    t0 = time.time()
    for key in REGISTRY:
        try:
            count = run_source(key, skip_download=skip_download)
            results[key] = count
        except Exception as e:
            print(f"\n[ERROR] {key} pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            results[key] = -1
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print("Pipeline summary:")
    for k, v in results.items():
        status = f"{v:,} upserted" if v >= 0 else "FAILED"
        print(f"  {k:<16} {status}")
    print(f"Total time: {elapsed:.1f}s")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Varro ingestion pipeline runner v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source",
        default="ALL",
        choices=list(REGISTRY.keys()) + ["ALL"],
        help="Which source to run (default: ALL)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing raw files)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available sources and exit",
    )
    args = parser.parse_args()

    if args.list:
        print("Available sources:")
        for k, v in REGISTRY.items():
            print(f"  {k:<16} → {v}")
        return

    if args.source == "ALL":
        run_all(skip_download=args.skip_download)
    else:
        try:
            count = run_source(args.source, skip_download=args.skip_download)
            print(f"\nResult: {count:,} records upserted for {args.source}")
        except Exception as e:
            print(f"[ERROR] {args.source} failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
