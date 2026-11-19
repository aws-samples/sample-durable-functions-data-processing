#!/usr/bin/env python3
"""One-command driver for the "a batch fails, then the retry succeeds" demo.

The failure needs no change to the Airport API environment — it is entirely
data-driven and heals on its own within one run:

  1. The enrichment function enriches each chunk against the Airport API with
     region=US on the FIRST map pass. The API serves only US airports in that
     scope and fails fast (HTTP 503 RegionNotCovered) for any non-US code, so a
     chunk containing an international flight fails.
  2. all_completed() isolates those failed chunks; the map-level retry pass
     re-runs ONLY them with region=INTL, which serves every airport — so the
     same chunks now succeed.

This script generates a dataset whose FIRST records are international (so the
failures land in the first few chunks and nowhere else), uploads it to the data
bucket, and prints the invoke command. Watch the manifest: ``failed_chunks`` is
empty at the end, but ``retried_chunks`` lists the chunks that failed the first
pass and recovered — the proof that the retry worked.

Reset back to clean, US-only data (no failures) with ``--reset``.

Examples
--------
    python scripts/demo_region.py                 # ~2 intl chunks fail then heal
    python scripts/demo_region.py --intl-count 3500 --chunk-size 1000  # ~4 chunks
    python scripts/demo_region.py --reset          # US-only data, nothing fails
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

try:
    import boto3
except ImportError:
    sys.exit("boto3 is required: pip install -r requirements-dev.txt")

# Call the generator in-process (it sits in the same scripts/ folder) instead
# of starting a subprocess.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate_flights  # noqa: E402

_DEFAULT_STACK = "Distributed-Functions-for-Data"


def _stack_output(cfn, stack: str, key: str) -> str | None:
    try:
        stacks = cfn.describe_stacks(StackName=stack)["Stacks"]
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read stack {stack}: {exc})", file=sys.stderr)
        return None
    for output in stacks[0].get("Outputs", []):
        if output["OutputKey"] == key:
            return output["OutputValue"]
    return None


def _run_generator(
    total: int, seed: int, bucket: str, key: str, intl_count: int | None
) -> None:
    argv = [
        "--count", str(total), "--seed", str(seed),
        "--bucket", bucket, "--key", key,
    ]
    if intl_count is not None:
        argv += ["--intl-count", str(intl_count)]
    else:
        # Clean reset: US-only data, no international flights at all.
        argv += ["--intl-rate", "0"]
    exit_code = generate_flights.main(argv)
    if exit_code:
        sys.exit(exit_code)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--intl-count", type=int, default=1500,
                        help="International flights concentrated in the leading records "
                             "(default: 1500). With --chunk-size 1000 that fails the first "
                             "2 chunks, which then heal on the retry pass.")
    parser.add_argument("--chunk-size", type=int, default=1000,
                        help="Chunk size you will invoke with (default: 1000). Only used to "
                             "report how many chunks will fail; pass the same value to invoke.")
    parser.add_argument("--total", type=int, default=100_000,
                        help="Total flights to generate (default: 100000).")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42).")
    parser.add_argument("--bucket", default=None,
                        help="Data bucket (default: FlightDataBucketName stack output).")
    parser.add_argument("--key", default="input/flights.jsonl", help="S3 key for the input.")
    parser.add_argument("--stack", default=_DEFAULT_STACK,
                        help=f"CloudFormation stack name (default: {_DEFAULT_STACK}).")
    parser.add_argument("--region", default=None, help="AWS region (default: profile/env).")
    parser.add_argument("--reset", action="store_true",
                        help="Generate clean US-only data (no international flights, nothing "
                             "fails).")
    args = parser.parse_args(argv)

    session = boto3.session.Session(region_name=args.region)
    cfn = session.client("cloudformation")

    bucket = args.bucket or _stack_output(cfn, args.stack, "FlightDataBucketName")
    if not bucket:
        sys.exit("Could not resolve the data bucket; pass --bucket explicitly.")

    if args.reset:
        print("Regenerating clean US-only data (no international flights)...")
        _run_generator(args.total, args.seed, bucket, args.key, intl_count=None)
        print("\nReset complete. The next run should complete with 0 failed and 0 "
              "retried chunks.")
        return 0

    failing_chunks = math.ceil(args.intl_count / args.chunk_size)
    print(
        f"Generating {args.total:,} flights with {args.intl_count} international "
        f"flight(s) in the leading records\n(fails the first {failing_chunks} chunk(s) "
        f"at chunk_size={args.chunk_size}, which then heal on the retry pass)..."
    )
    _run_generator(args.total, args.seed, bucket, args.key, intl_count=args.intl_count)

    alias = _stack_output(cfn, args.stack, "LargeScaleProcessingAliasArn") or "<alias-arn>"
    print("\nReady. Start the durable execution:\n")
    print(
        f"  aws lambda invoke --function-name {alias} \\\n"
        f"    --invocation-type Event \\\n"
        f"    --payload '{{\"input_key\":\"{args.key}\",\"chunk_size\":{args.chunk_size},"
        f"\"max_concurrency\":8}}' \\\n"
        f"    --cli-binary-format raw-in-base64-out response.json\n"
    )
    print(
        "Then check the manifest: 'retried_chunks' lists the chunks that failed the\n"
        "US-scoped first pass and recovered on the INTL retry, and 'failed_chunks' is\n"
        "empty — the run still completes.\n"
        "Run 'python scripts/demo_region.py --reset' to return to clean US-only data."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
