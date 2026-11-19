"""Mock Airport reference API (AWS Lambda, invoked via a Function URL).

This is the "downstream service" the durable map function enriches against.
It is deliberately small: it loads a bundled airport reference dataset once
per execution environment and answers point lookups by IATA code.

Supported requests (HTTP API v2 / Function URL payload format 2.0):

    GET /airports/{code}          -> 200 airport object, or 404 if unknown
    GET /airports?codes=AAA,BBB   -> 200 {"results": {...}, "unknown": [...]}
    GET /health                   -> 200 {"status": "ok", "airports": N}

Request scoping (query parameter):

    region=INTL         Serve every airport (default when the parameter is
                        absent). This is the "global" scope.
    region=US           Serve only US airports. If the request references any
                        non-US code the API fails fast with 503
                        "RegionNotCovered" and lists the offending codes, so a
                        caller must widen its scope (region=INTL) to resolve
                        them. This powers the "a batch fails on the first pass,
                        then the retry pass widens scope and succeeds" demo.

Demo knobs (environment variables):

    DEMO_LATENCY_MS     Sleep this many milliseconds before responding, to
                        simulate a slow downstream dependency.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

# Load the reference dataset once per execution environment (module scope), so
# only cold starts pay the read/parse cost.
_DATA_PATH = Path(__file__).with_name("airports.json")
with _DATA_PATH.open(encoding="utf-8") as fh:
    AIRPORTS: dict[str, dict] = json.load(fh)

_LATENCY_MS = int(os.environ.get("DEMO_LATENCY_MS", "0"))


def _response(status: int, body: dict) -> dict:
    """Build an HTTP API v2 proxy response."""
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def _airport(code: str) -> dict:
    """Return the airport record for a code, annotated with the code itself."""
    record = AIRPORTS[code]
    return {"code": code, **record}


def _maybe_out_of_region(codes: list[str], region: str) -> dict | None:
    """Fail fast (503) when a US-scoped request references any non-US code.

    region=US serves only US airports; a request that includes a code for an
    airport in another country is "not covered" by this scope. We return 503
    with a distinct ``RegionNotCovered`` error so the caller can recognize it
    and retry with a wider scope (region=INTL) rather than treating it as a
    transient blip. region=INTL (the default) covers everything, so it never
    trips this.
    """
    if region != "US":
        return None
    out_of_region = sorted(
        code for code in set(codes)
        if code in AIRPORTS and AIRPORTS[code].get("country") != "US"
    )
    if out_of_region:
        return _response(
            503,
            {"error": "RegionNotCovered", "region": region, "codes": out_of_region,
             "message": (
                 f"Region '{region}' does not cover non-US airport(s): "
                 f"{', '.join(out_of_region)}. Retry with region=INTL."
             )},
        )
    return None


def lambda_handler(event: dict, _context) -> dict:
    if _LATENCY_MS:
        time.sleep(_LATENCY_MS / 1000.0)

    method = (
        event.get("requestContext", {}).get("http", {}).get("method", "GET").upper()
    )
    raw_path = event.get("rawPath", "/") or "/"
    query = event.get("queryStringParameters") or {}
    # Scope of this request. Default INTL (serve everything); US restricts to
    # US airports and fails fast on anything else.
    region = (query.get("region") or "INTL").strip().upper()

    if raw_path == "/health":
        return _response(200, {"status": "ok", "airports": len(AIRPORTS)})

    if method != "GET":
        return _response(405, {"error": "MethodNotAllowed", "method": method})

    # Batch lookup: GET /airports?codes=AAA,BBB,CCC[&region=US|INTL]
    if raw_path.rstrip("/") == "/airports" and "codes" in query:
        codes = [c.strip().upper() for c in query["codes"].split(",") if c.strip()]
        failure = _maybe_out_of_region(codes, region)
        if failure is not None:
            return failure
        results = {c: _airport(c) for c in codes if c in AIRPORTS}
        unknown = [c for c in codes if c not in AIRPORTS]
        return _response(200, {"results": results, "unknown": unknown})

    # Point lookup: GET /airports/{code}[?region=US|INTL]
    parts = [p for p in raw_path.split("/") if p]
    if len(parts) == 2 and parts[0] == "airports":
        code = parts[1].upper()
        failure = _maybe_out_of_region([code], region)
        if failure is not None:
            return failure
        if code not in AIRPORTS:
            return _response(404, {"error": "NotFound", "code": code})
        return _response(200, _airport(code))

    return _response(404, {"error": "NotFound", "path": raw_path})
