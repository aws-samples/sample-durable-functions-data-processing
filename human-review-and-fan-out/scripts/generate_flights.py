#!/usr/bin/env python3
"""Generate a realistic-ish JSONL dataset of airline flights.

Each line is one flight record with scheduled and (usually) actual times, an
origin/destination IATA code, a flight number, and the operating airline. The
durable map function enriches these against the mock Airport API.

The airport reference dataset is the single source of truth shared with the
Airport API lambda: ``lambda/airport_api/airports.json``.

By default the dataset is uploaded straight to the demo's data bucket, which is
resolved automatically from the ``FlightDataBucketName`` output of the deployed
CloudFormation stack (``Distributed-Functions-for-Data``). Pass ``--bucket`` to
override the target, or ``--no-upload`` to only write the file locally.

Examples
--------
Generate 100k flights and upload to the deployed stack's bucket (deterministic)::

    python scripts/generate_flights.py --count 100000 --seed 42

Generate 5k flights spread over a week, local file only (no upload)::

    python scripts/generate_flights.py --count 5000 --days 7 --no-upload

Upload to a specific bucket/key instead of the stack's::

    python scripts/generate_flights.py --count 5000 \\
        --bucket my-data-bucket --key input/flights.jsonl

Sprinkle in 1% of records with unknown airport codes to exercise the map's
record-level error handling::

    python scripts/generate_flights.py --count 10000 --corrupt-rate 0.01 --no-upload
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import random
import sys
from pathlib import Path

# scripts/ -> sample root -> the airport dataset bundled with the API lambda.
_SAMPLE_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_AIRPORTS = _SAMPLE_ROOT / "lambda" / "airport_api" / "airports.json"
_DEFAULT_OUTPUT = _SAMPLE_ROOT / "data" / "flights.jsonl"

# CloudFormation stack the bucket is resolved from when --bucket is omitted.
_DEFAULT_STACK = "Distributed-Functions-for-Data"
_BUCKET_OUTPUT_KEY = "FlightDataBucketName"


def _stack_output(stack: str, key: str, region: str | None) -> str | None:
    """Return a CloudFormation stack output value, or None if unavailable.

    boto3 is imported lazily so the generator keeps its stdlib-only core; the
    dependency only matters when actually talking to AWS.
    """
    try:
        import boto3  # noqa: PLC0415
    except ImportError:
        print("  (boto3 not installed; cannot resolve bucket from the stack)",
              file=sys.stderr)
        return None
    cfn = boto3.session.Session(region_name=region).client("cloudformation")
    try:
        stacks = cfn.describe_stacks(StackName=stack)["Stacks"]
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read stack {stack}: {exc})", file=sys.stderr)
        return None
    for output in stacks[0].get("Outputs", []):
        if output["OutputKey"] == key:
            return output["OutputValue"]
    return None

# (IATA code, display name). Kept intentionally small and recognizable.
AIRLINES: list[tuple[str, str]] = [
    ("DL", "Delta Air Lines"), ("AA", "American Airlines"), ("UA", "United Airlines"),
    ("WN", "Southwest Airlines"), ("AC", "Air Canada"), ("B6", "JetBlue Airways"),
    ("AS", "Alaska Airlines"), ("BA", "British Airways"), ("LH", "Lufthansa"),
    ("AF", "Air France"), ("KL", "KLM"), ("IB", "Iberia"), ("AZ", "ITA Airways"),
    ("LX", "SWISS"), ("TK", "Turkish Airlines"), ("EK", "Emirates"),
    ("QR", "Qatar Airways"), ("EY", "Etihad Airways"), ("SQ", "Singapore Airlines"),
    ("CX", "Cathay Pacific"), ("QF", "Qantas"), ("NH", "All Nippon Airways"),
    ("JL", "Japan Airlines"), ("KE", "Korean Air"), ("CA", "Air China"),
    ("LA", "LATAM Airlines"), ("ET", "Ethiopian Airlines"), ("SA", "South African Airways"),
]

# Cruise ground speed and fixed block overhead (taxi + climb + descent), used to
# turn great-circle distance into a plausible scheduled block time.
_CRUISE_KMH = 820.0
_BLOCK_OVERHEAD_MIN = 35

_BOGUS_CODES = ["ZZZ", "QQQ", "XYZ", "000"]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two lat/lon points, in kilometers."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def block_minutes(distance_km: float) -> int:
    """Scheduled gate-to-gate block time for a distance, rounded to 5 minutes."""
    minutes = _BLOCK_OVERHEAD_MIN + (distance_km / _CRUISE_KMH) * 60.0
    return max(30, int(round(minutes / 5.0)) * 5)


def _iso_z(moment: dt.datetime) -> str:
    """Format a UTC datetime as ISO 8601 with a trailing Z, second precision."""
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sample_departure_delay(rng: random.Random) -> int:
    """Sample a departure delay in minutes from a long-tailed distribution."""
    roll = rng.random()
    if roll < 0.65:
        return rng.randint(-5, 10)      # on time (or a little early)
    if roll < 0.87:
        return rng.randint(11, 45)      # minor delay
    if roll < 0.97:
        return rng.randint(46, 120)     # significant delay
    return rng.randint(121, 300)        # severe delay


def _weighted_codes(
    airports: dict[str, dict], country: str | None = None
) -> tuple[list[str], list[float]]:
    """Codes and selection weights, optionally restricted to one country.

    ``country="US"`` returns only US airports; ``None`` returns all. Used to
    keep most flights domestic (US) and sprinkle in a tunable fraction of
    international ones.
    """
    codes = [
        c for c in airports
        if country is None or airports[c].get("country") == country
    ]
    weights = [float(airports[c].get("weight", 1)) for c in codes]
    return codes, weights


def _pick(rng: random.Random, codes: list[str], weights: list[float]) -> str:
    return rng.choices(codes, weights=weights, k=1)[0]


def generate_flight(
    rng: random.Random,
    airports: dict[str, dict],
    us_codes: list[str],
    us_weights: list[float],
    intl_codes: list[str],
    intl_weights: list[float],
    window_start: dt.datetime,
    window_seconds: int,
    corrupt_rate: float,
    intl_rate: float,
) -> dict:
    """Build a single flight record.

    Most flights are US-to-US. With probability ``intl_rate`` the flight is
    "international": one endpoint is forced to a non-US airport, so the chunk it
    lands in fails the US-scoped first map pass and recovers on the INTL retry.
    """
    if intl_codes and rng.random() < intl_rate:
        # International flight: one US endpoint, one non-US endpoint (order
        # randomized so either origin or destination is the foreign one).
        us_airport = _pick(rng, us_codes, us_weights)
        foreign_airport = _pick(rng, intl_codes, intl_weights)
        origin, destination = (
            (us_airport, foreign_airport)
            if rng.random() < 0.5
            else (foreign_airport, us_airport)
        )
    else:
        # Domestic US flight.
        origin = _pick(rng, us_codes, us_weights)
        destination = _pick(rng, us_codes, us_weights)
        while destination == origin:
            destination = _pick(rng, us_codes, us_weights)

    airline_code, airline_name = rng.choice(AIRLINES)
    flight_number = f"{airline_code}{rng.randint(1, 4999)}"

    o, d = airports[origin], airports[destination]
    distance = haversine_km(o["lat"], o["lon"], d["lat"], d["lon"])
    block = block_minutes(distance)

    sched_dep = window_start + dt.timedelta(seconds=rng.randint(0, window_seconds))
    sched_arr = sched_dep + dt.timedelta(minutes=block)

    record: dict = {
        "flight_number": flight_number,
        "airline": airline_name,
        "origin": origin,
        "destination": destination,
        "scheduled_departure": _iso_z(sched_dep),
        "scheduled_arrival": _iso_z(sched_arr),
    }

    # ~1.5% of flights are cancelled: no actual times.
    if rng.random() < 0.015:
        record["actual_departure"] = None
        record["actual_arrival"] = None
    else:
        dep_delay = _sample_departure_delay(rng)
        # Some delay is recovered/added enroute; keep total arrival >= departure-ish.
        arr_delay = dep_delay + rng.randint(-10, 15)
        actual_dep = sched_dep + dt.timedelta(minutes=dep_delay)
        actual_arr = sched_arr + dt.timedelta(minutes=arr_delay)
        record["actual_departure"] = _iso_z(actual_dep)
        record["actual_arrival"] = _iso_z(actual_arr)

    # Optionally corrupt a code so the enrichment step must handle unknowns.
    if corrupt_rate > 0 and rng.random() < corrupt_rate:
        field = rng.choice(["origin", "destination"])
        record[field] = rng.choice(_BOGUS_CODES)

    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=100_000,
                        help="Number of flight records to generate (default: 100000).")
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT,
                        help=f"Output JSONL path (default: {_DEFAULT_OUTPUT}).")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed for reproducible output.")
    parser.add_argument("--start-date", type=str, default=None,
                        help="UTC window start, YYYY-MM-DD (default: today).")
    parser.add_argument("--days", type=int, default=1,
                        help="Spread scheduled departures over this many days (default: 1).")
    parser.add_argument("--corrupt-rate", type=float, default=0.0,
                        help="Fraction [0,1] of records given an unknown airport code "
                             "(record-level enrichment errors; the chunk still succeeds).")
    parser.add_argument("--intl-rate", type=float, default=0.02,
                        help="Fraction [0,1] of flights that are international (one non-US "
                             "airport), spread UNIFORMLY. Non-US airports fail the US-scoped "
                             "first map pass and recover on the INTL retry pass. Spread "
                             "uniformly, even a small rate puts an intl flight in every chunk, "
                             "so EVERY chunk fails the first pass. For 'only a few chunks fail', "
                             "use --intl-count instead (default: 0.02). Set 0 for US-only data.")
    parser.add_argument("--intl-count", type=int, default=None,
                        help="Concentrate exactly N international flights into the FIRST N "
                             "records (overrides --intl-rate). They land in the first "
                             "ceil(N / chunk_size) chunks, so only those few chunks fail the "
                             "US-scoped pass and recover on the INTL retry — the rest stay "
                             "green. This is the knob for the 'batch fails then retry succeeds' "
                             "demo (e.g. --intl-count 1500 with chunk_size 1000 fails 2 chunks).")
    parser.add_argument("--airports", type=Path, default=_DEFAULT_AIRPORTS,
                        help="Path to the airport reference JSON.")
    parser.add_argument("--bucket", type=str, default=None,
                        help="Target S3 bucket. Defaults to the stack's "
                             f"{_BUCKET_OUTPUT_KEY} output (see --stack).")
    parser.add_argument("--key", type=str, default="input/flights.jsonl",
                        help="S3 key to upload to (default: input/flights.jsonl).")
    parser.add_argument("--no-upload", action="store_true",
                        help="Only write the local file; skip the S3 upload.")
    parser.add_argument("--stack", type=str, default=_DEFAULT_STACK,
                        help="CloudFormation stack to resolve the bucket from when "
                             f"--bucket is omitted (default: {_DEFAULT_STACK}).")
    parser.add_argument("--region", type=str, default=None,
                        help="AWS region for stack lookup / upload (default: profile/env).")
    args = parser.parse_args(argv)

    if not 0.0 <= args.corrupt_rate <= 1.0:
        parser.error("--corrupt-rate must be between 0 and 1")
    if not 0.0 <= args.intl_rate <= 1.0:
        parser.error("--intl-rate must be between 0 and 1")
    if args.intl_count is not None and args.intl_count < 0:
        parser.error("--intl-count must be non-negative")

    airports: dict[str, dict] = json.loads(args.airports.read_text(encoding="utf-8"))
    if len(airports) < 2:
        parser.error(f"airport dataset {args.airports} needs at least 2 airports")
    us_codes, us_weights = _weighted_codes(airports, country="US")
    intl_codes, intl_weights = _weighted_codes(
        airports, country=None
    )
    # The international pool is the non-US airports only.
    intl_pairs = [
        (c, w) for c, w in zip(intl_codes, intl_weights)
        if airports[c].get("country") != "US"
    ]
    intl_codes = [c for c, _ in intl_pairs]
    intl_weights = [w for _, w in intl_pairs]
    if len(us_codes) < 2:
        parser.error(
            f"airport dataset {args.airports} needs at least 2 US airports for "
            "domestic flights"
        )
    if args.intl_rate > 0 and not intl_codes:
        parser.error(
            f"--intl-rate > 0 but airport dataset {args.airports} has no non-US airports"
        )

    if args.start_date:
        start = dt.datetime.strptime(args.start_date, "%Y-%m-%d").replace(
            tzinfo=dt.timezone.utc
        )
    else:
        start = dt.datetime.now(dt.timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    window_seconds = max(1, args.days) * 86_400

    rng = random.Random(args.seed)  # nosec B311 - synthetic test data

    args.output.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with args.output.open("w", encoding="utf-8") as out:
        for i in range(args.count):
            # --intl-count concentrates intl flights into the leading records:
            # force the first N international, the rest domestic (ignore the
            # uniform rate). Otherwise use the uniform --intl-rate.
            if args.intl_count is not None:
                effective_intl_rate = 1.0 if i < args.intl_count else 0.0
            else:
                effective_intl_rate = args.intl_rate
            record = generate_flight(
                rng, airports,
                us_codes, us_weights, intl_codes, intl_weights,
                start, window_seconds, args.corrupt_rate, effective_intl_rate,
            )
            out.write(json.dumps(record) + "\n")
            written += 1
            if written % 50_000 == 0:
                print(f"  ... {written:,} / {args.count:,}", file=sys.stderr)

    if args.intl_count is not None:
        print(
            f"Placed {min(args.intl_count, written)} international flight(s) in the "
            f"leading records (concentrated into the first chunks)."
        )

    size_mb = args.output.stat().st_size / (1024 * 1024)
    print(f"Wrote {written:,} flights to {args.output} ({size_mb:.1f} MiB)")

    if args.no_upload:
        return 0

    # Resolve the target bucket: an explicit --bucket wins; otherwise pull it
    # from the deployed stack's FlightDataBucketName output.
    bucket = args.bucket
    if not bucket:
        print(f"Resolving data bucket from stack {args.stack} ...")
        bucket = _stack_output(args.stack, _BUCKET_OUTPUT_KEY, args.region)
    if not bucket:
        parser.error(
            "Could not determine the target bucket. Deploy the stack first, "
            "pass --bucket explicitly, or use --no-upload to write only the "
            "local file."
        )

    try:
        import boto3  # imported lazily so the generator has no hard dependency
    except ImportError:
        parser.error("boto3 is required to upload (pip install -r requirements-dev.txt)")
    s3 = boto3.session.Session(region_name=args.region).client("s3")
    print(f"Uploading to s3://{bucket}/{args.key} ...")
    s3.upload_file(str(args.output), bucket, args.key)
    print(f"Uploaded s3://{bucket}/{args.key}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
