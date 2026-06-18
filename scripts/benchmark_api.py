#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "click"]
# ///
"""
Benchmark disk-tree API endpoints to catch performance regressions.

Usage:
    ./scripts/benchmark_api.py                    # Run all benchmarks
    ./scripts/benchmark_api.py --endpoint scan    # Run specific endpoint
    ./scripts/benchmark_api.py --save             # Save results as baseline
    ./scripts/benchmark_api.py --compare          # Compare against baseline
"""

import json
import sys
import time
from pathlib import Path
from statistics import mean, stdev

import click
import requests

BASE_URL = "http://localhost:5001"
BASELINE_FILE = Path(__file__).parent.parent / "tmp" / "benchmark_baseline.json"

# Thresholds (seconds) - fail if exceeded
THRESHOLDS = {
    "scans_list": 0.5,
    "scan_root": 1.0,
    "scan_subdir": 1.0,
    "scan_deep_subdir": 1.0,
    "history": 1.0,
    "compare": 1.0,
}


def time_request(url: str, params: dict | None = None, runs: int = 3) -> dict:
    """Time a request multiple times and return stats."""
    times = []
    last_status = None
    last_error = None

    for i in range(runs):
        try:
            start = time.perf_counter()
            resp = requests.get(url, params=params, timeout=30)
            elapsed = time.perf_counter() - start
            times.append(elapsed)
            last_status = resp.status_code
            if resp.status_code != 200:
                last_error = resp.text[:200]
        except Exception as e:
            last_error = str(e)

    if not times:
        return {"error": last_error, "times": [], "mean": None, "status": last_status}

    return {
        "times": times,
        "mean": mean(times),
        "stdev": stdev(times) if len(times) > 1 else 0,
        "min": min(times),
        "max": max(times),
        "status": last_status,
        "error": last_error if last_status != 200 else None,
    }


def get_test_params(db_path: str | None = None) -> dict:
    """Get test parameters from the database."""
    # Try to get scan IDs and paths from the API
    try:
        resp = requests.get(f"{BASE_URL}/api/scans", timeout=10)
        if resp.status_code == 200:
            scans = resp.json()
            if scans:
                # Find a local scan (not S3)
                local_scans = [s for s in scans if not s["path"].startswith("s3://")]
                if local_scans:
                    # Get the largest scan for more realistic testing
                    largest = max(local_scans, key=lambda s: s.get("n_desc") or 0)
                    scan_path = largest["path"]

                    # Find two scans of the same path for compare test
                    resp2 = requests.get(
                        f"{BASE_URL}/api/scans/history",
                        params={"uri": scan_path},
                        timeout=10,
                    )
                    if resp2.status_code == 200:
                        history = resp2.json()
                        if len(history) >= 2:
                            return {
                                "scan_path": scan_path,
                                "scan1_id": history[1]["id"],
                                "scan2_id": history[0]["id"],
                                "subdir": f"{scan_path}/Library" if "Users" in scan_path else scan_path,
                                "deep_subdir": f"{scan_path}/Library/Application Support" if "Users" in scan_path else scan_path,
                            }
    except Exception as e:
        print(f"Warning: Could not auto-detect test params: {e}", file=sys.stderr)

    # Fallback defaults
    return {
        "scan_path": "/Users/ryan",
        "scan1_id": 56,
        "scan2_id": 59,
        "subdir": "/Users/ryan/Library",
        "deep_subdir": "/Users/ryan/Library/Application Support",
    }


def run_benchmarks(params: dict, runs: int = 3) -> dict:
    """Run all benchmark tests."""
    results = {}

    # 1. List all scans
    print("  scans_list: ", end="", flush=True)
    results["scans_list"] = time_request(f"{BASE_URL}/api/scans", runs=runs)
    print(f"{results['scans_list']['mean']:.3f}s" if results['scans_list']['mean'] else "FAILED")

    # 2. Get scan at root path
    print("  scan_root: ", end="", flush=True)
    results["scan_root"] = time_request(
        f"{BASE_URL}/api/scan",
        params={"uri": params["scan_path"]},
        runs=runs,
    )
    print(f"{results['scan_root']['mean']:.3f}s" if results['scan_root']['mean'] else "FAILED")

    # 3. Get scan at subdir (uses ancestor scan)
    print("  scan_subdir: ", end="", flush=True)
    results["scan_subdir"] = time_request(
        f"{BASE_URL}/api/scan",
        params={"uri": params["subdir"]},
        runs=runs,
    )
    print(f"{results['scan_subdir']['mean']:.3f}s" if results['scan_subdir']['mean'] else "FAILED")

    # 4. Get scan at deep subdir
    print("  scan_deep_subdir: ", end="", flush=True)
    results["scan_deep_subdir"] = time_request(
        f"{BASE_URL}/api/scan",
        params={"uri": params["deep_subdir"]},
        runs=runs,
    )
    print(f"{results['scan_deep_subdir']['mean']:.3f}s" if results['scan_deep_subdir']['mean'] else "FAILED")

    # 5. Get scan history (includes ancestor scans)
    print("  history: ", end="", flush=True)
    results["history"] = time_request(
        f"{BASE_URL}/api/scans/history",
        params={"uri": params["deep_subdir"]},
        runs=runs,
    )
    print(f"{results['history']['mean']:.3f}s" if results['history']['mean'] else "FAILED")

    # 6. Compare two scans
    print("  compare: ", end="", flush=True)
    results["compare"] = time_request(
        f"{BASE_URL}/api/compare",
        params={
            "uri": params["deep_subdir"],
            "scan1": params["scan1_id"],
            "scan2": params["scan2_id"],
        },
        runs=runs,
    )
    print(f"{results['compare']['mean']:.3f}s" if results['compare']['mean'] else "FAILED")

    return results


def check_thresholds(results: dict) -> list[str]:
    """Check if any results exceed thresholds."""
    failures = []
    for name, threshold in THRESHOLDS.items():
        if name in results and results[name].get("mean"):
            if results[name]["mean"] > threshold:
                failures.append(
                    f"{name}: {results[name]['mean']:.3f}s > {threshold}s threshold"
                )
    return failures


def compare_to_baseline(results: dict, baseline: dict) -> list[str]:
    """Compare results to baseline and report regressions."""
    regressions = []
    for name in results:
        if name in baseline and results[name].get("mean") and baseline[name].get("mean"):
            current = results[name]["mean"]
            base = baseline[name]["mean"]
            if current > base * 1.5:  # 50% regression threshold
                regressions.append(
                    f"{name}: {current:.3f}s vs baseline {base:.3f}s ({current/base:.1f}x slower)"
                )
    return regressions


@click.command()
@click.option("-e", "--endpoint", help="Run specific endpoint benchmark only")
@click.option("-r", "--runs", default=3, help="Number of runs per benchmark")
@click.option("-s", "--save", is_flag=True, help="Save results as new baseline")
@click.option("-c", "--compare", is_flag=True, help="Compare against baseline")
@click.option("-v", "--verbose", is_flag=True, help="Show detailed output")
def main(endpoint: str | None, runs: int, save: bool, compare: bool, verbose: bool):
    """Benchmark disk-tree API endpoints."""
    print("Disk-tree API Benchmark")
    print("=" * 40)

    # Check server is running
    try:
        requests.get(f"{BASE_URL}/api/scans", timeout=2)
    except Exception:
        print(f"Error: Server not running at {BASE_URL}", file=sys.stderr)
        print("Start with: disk-tree-server", file=sys.stderr)
        sys.exit(1)

    # Get test parameters
    print("\nDetecting test parameters...")
    params = get_test_params()
    if verbose:
        print(f"  scan_path: {params['scan_path']}")
        print(f"  scan IDs: {params['scan1_id']}, {params['scan2_id']}")
        print(f"  subdir: {params['subdir']}")
        print(f"  deep_subdir: {params['deep_subdir']}")

    # Run benchmarks
    print(f"\nRunning benchmarks ({runs} runs each)...")
    results = run_benchmarks(params, runs=runs)

    # Check thresholds
    print("\nThreshold check:")
    failures = check_thresholds(results)
    if failures:
        for f in failures:
            print(f"  FAIL: {f}")
    else:
        print("  All endpoints within thresholds")

    # Compare to baseline
    if compare and BASELINE_FILE.exists():
        print("\nBaseline comparison:")
        baseline = json.loads(BASELINE_FILE.read_text())
        regressions = compare_to_baseline(results, baseline)
        if regressions:
            for r in regressions:
                print(f"  REGRESSION: {r}")
        else:
            print("  No significant regressions")

    # Save baseline
    if save:
        BASELINE_FILE.parent.mkdir(exist_ok=True)
        BASELINE_FILE.write_text(json.dumps(results, indent=2))
        print(f"\nBaseline saved to {BASELINE_FILE}")

    # Summary
    print("\nSummary:")
    for name, data in results.items():
        if data.get("mean"):
            status = "OK" if name not in [f.split(":")[0] for f in failures] else "SLOW"
            print(f"  {name}: {data['mean']:.3f}s [{status}]")
        else:
            print(f"  {name}: FAILED ({data.get('error', 'unknown error')})")

    # Exit with error if thresholds exceeded
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
