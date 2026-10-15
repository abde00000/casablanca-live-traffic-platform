"""Poll TomTom's live Flow Segment API and publish real responses to Kafka.

This module intentionally has no synthetic-data fallback. The API key is read
from the environment and is never included in logs or the Kafka event payload.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

LOG = logging.getLogger("casablanca_traffic_collector")
STOP_EVENT = threading.Event()
DEFAULT_API_URL = (
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
)
RETRYABLE_HTTP = {429, 500, 502, 503, 504}


class BudgetExceeded(RuntimeError):
    """Raised before a request would exceed the configured monthly call budget."""


class ProviderAuthenticationError(RuntimeError):
    """A sanitized provider authentication/quota error; never contains the URL/key."""


class MonthlyRequestBudget:
    """Small local guardrail; not a replacement for the provider account quota."""

    def __init__(self, state_file: str | Path, monthly_limit: int):
        if monthly_limit < 1:
            raise ValueError("monthly_limit must be positive")
        self.state_file = Path(state_file)
        self.monthly_limit = monthly_limit

    def _read_state(self, month_key: str) -> int:
        try:
            state = json.loads(self.state_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return 0
        except (OSError, json.JSONDecodeError):
            LOG.warning("Budget state unreadable; refusing to assume zero usage")
            raise RuntimeError("Cannot safely read local API request budget state")
        if state.get("month_utc") != month_key:
            return 0
        return int(state.get("requests_started", 0))

    def reserve(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        month_key = now.astimezone(timezone.utc).strftime("%Y-%m")
        count = self._read_state(month_key)
        if count >= self.monthly_limit:
            raise BudgetExceeded(
                f"Local monthly request cap reached ({self.monthly_limit}); "
                "raise it only after checking the provider account limit."
            )
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        state = {"month_utc": month_key, "requests_started": count + 1}
        temp_file = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        temp_file.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        os.replace(temp_file, self.state_file)
        return count + 1


def load_locations(path: str | Path) -> list[dict[str, Any]]:
    """Load the OSM-sourced location catalogue and validate its identifiers."""
    required = {
        "location_id", "arrondissement", "road_name_osm", "latitude", "longitude",
        "osm_way_id", "osm_relation_id", "osm_highway_class",
    }
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"Location CSV must contain columns: {sorted(required)}")
        for line_no, raw in enumerate(reader, start=2):
            location_id = (raw.get("location_id") or "").strip()
            if not location_id or location_id in seen:
                raise ValueError(f"Missing or duplicate location_id at CSV line {line_no}")
            seen.add(location_id)
            try:
                latitude = float(raw["latitude"])
                longitude = float(raw["longitude"])
                osm_way_id = int(raw["osm_way_id"])
                osm_relation_id = int(raw["osm_relation_id"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid coordinates or OSM IDs at CSV line {line_no}") from exc
            if not math.isfinite(latitude) or not -90 <= latitude <= 90:
                raise ValueError(f"Invalid latitude at CSV line {line_no}")
            if not math.isfinite(longitude) or not -180 <= longitude <= 180:
                raise ValueError(f"Invalid longitude at CSV line {line_no}")
            for field in ("arrondissement", "road_name_osm", "osm_highway_class"):
                if not (raw.get(field) or "").strip():
                    raise ValueError(f"Missing {field} at CSV line {line_no}")
            rows.append({
                **{key: value.strip() if isinstance(value, str) else value for key, value in raw.items()},
                "location_id": location_id,
                "latitude": latitude,
                "longitude": longitude,
                "osm_way_id": osm_way_id,
                "osm_relation_id": osm_relation_id,
            })
    if not rows:
        raise ValueError("Location CSV contains no locations")
    return rows


def _coordinates_from_payload(flow: dict[str, Any]) -> list[dict[str, float]]:
    coordinates = flow.get("coordinates", {}).get("coordinate", [])
    if isinstance(coordinates, dict):
        coordinates = [coordinates]
    result = []
    for item in coordinates if isinstance(coordinates, list) else []:
        if not isinstance(item, dict):
            continue
        try:
            result.append({
                "latitude": float(item["latitude"]),
                "longitude": float(item["longitude"]),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return result


def normalize_response(
    location: dict[str, Any],
    payload: dict[str, Any],
    collected_at: datetime | None = None,
    store_raw_response: bool = False,
) -> dict[str, Any]:
    """Attach provenance/collection metadata to one successful provider response."""
    flow = payload.get("flowSegmentData")
    if not isinstance(flow, dict):
        raise ValueError("Provider response has no flowSegmentData object")
    required = (
        "currentSpeed", "freeFlowSpeed", "currentTravelTime",
        "freeFlowTravelTime", "confidence",
    )
    missing = [key for key in required if flow.get(key) is None]
    if missing:
        raise ValueError("Provider response missing required flow fields: " + ",".join(missing))

    timestamp = collected_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("collected_at must be timezone-aware")
    timestamp = timestamp.astimezone(timezone.utc)
    canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    event: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "provider": "TomTom",
        "product": "Traffic Flow Segment Data",
        "location_id": location["location_id"],
        "arrondissement": location["arrondissement"],
        "road_name_osm": location["road_name_osm"],
        "request_latitude": float(location["latitude"]),
        "request_longitude": float(location["longitude"]),
        "osm_way_id": int(location["osm_way_id"]),
        "osm_relation_id": int(location["osm_relation_id"]),
        "osm_highway_class": location["osm_highway_class"],
        "collected_at_utc": timestamp.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "current_speed_kmh": float(flow["currentSpeed"]),
        "free_flow_speed_kmh": float(flow["freeFlowSpeed"]),
        "current_travel_time_s": float(flow["currentTravelTime"]),
        "free_flow_travel_time_s": float(flow["freeFlowTravelTime"]),
        "confidence": float(flow["confidence"]),
        "road_closure": flow.get("roadClosure"),
        "frc": flow.get("frc"),
        "openlr": flow.get("openlr"),
        "provider_segment_coordinates": _coordinates_from_payload(flow),
        "provider_api_version": flow.get("@version") or flow.get("-version"),
        "provider_payload_sha256": hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest(),
    }
    if store_raw_response:
        event["provider_response"] = payload
    return event


def fetch_response(
    location: dict[str, Any],
    api_key: str,
    budget: MonthlyRequestBudget,
    endpoint: str = DEFAULT_API_URL,
    timeout_seconds: int = 20,
    max_retries: int = 3,
) -> dict[str, Any] | None:
    """Fetch one real response; never logs the request URL or API key."""
    params = urlencode({
        "point": f"{location['latitude']:.6f},{location['longitude']:.6f}",
        "unit": "KMPH",
        "openLr": "true",
        "key": api_key,
    })
    request = Request(
        f"{endpoint}?{params}",
        headers={"Accept": "application/json", "User-Agent": "CasablancaLiveTrafficAcademicProject/1.0"},
    )
    for attempt in range(max_retries + 1):
        budget.reserve()
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                decoded = json.loads(response.read().decode("utf-8"))
                if not isinstance(decoded, dict):
                    LOG.error("TomTom returned a non-object JSON response: location_id=%s", location["location_id"])
                    return None
                return decoded
        except HTTPError as exc:
            status = int(exc.code)
            if status in (401, 403):
                raise ProviderAuthenticationError(
                    f"TomTom returned HTTP {status}; verify key access and account quota."
                ) from None
            if status in RETRYABLE_HTTP and attempt < max_retries:
                time.sleep(min(2 ** attempt, 30))
                continue
            LOG.error("TomTom request failed: status=%s location_id=%s", status, location["location_id"])
            return None
        except (URLError, TimeoutError, OSError) as exc:
            if attempt < max_retries:
                time.sleep(min(2 ** attempt, 30))
                continue
            LOG.error(
                "TomTom request failed after retries: location_id=%s error_type=%s",
                location["location_id"], type(exc).__name__,
            )
            return None
        except (UnicodeDecodeError, json.JSONDecodeError):
            LOG.error("TomTom returned invalid JSON: location_id=%s", location["location_id"])
            return None
    return None


def _handle_signal(signum: int, _frame: Any) -> None:
    LOG.info("Received signal %s; stopping after current request", signum)
    STOP_EVENT.set()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    api_key = os.getenv("TOMTOM_API_KEY", "").strip()
    if not api_key or api_key.lower().startswith("replace_"):
        raise SystemExit("TOMTOM_API_KEY is required; no sample or synthetic data will be emitted.")

    locations_file = os.getenv("TRAFFIC_LOCATIONS_FILE", "/app/config/traffic_locations.csv")
    locations = load_locations(locations_file)
    endpoint = os.getenv("TOMTOM_FLOW_API_URL", DEFAULT_API_URL).strip()
    topic = os.getenv("KAFKA_TOPIC", "traffic-flow-raw")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
    interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
    timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    monthly_limit = int(os.getenv("MAX_REQUESTS_PER_MONTH", "18000"))
    budget_file = os.getenv("REQUEST_BUDGET_FILE", "/data/api_request_budget.json")
    store_raw = os.getenv("STORE_RAW_PROVIDER_RESPONSE", "false").strip().lower() in {"1", "true", "yes"}
    if interval_seconds < 60:
        raise SystemExit("POLL_INTERVAL_SECONDS must be at least 60 to avoid aggressive polling.")

    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit("Install requirements.txt before starting the collector.") from exc

    budget = MonthlyRequestBudget(budget_file, monthly_limit)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        max_block_ms=30000,
    )
    LOG.info(
        "Collector ready: locations=%s interval_seconds=%s topic=%s raw_response_storage=%s",
        len(locations), interval_seconds, topic, store_raw,
    )

    try:
        while not STOP_EVENT.is_set():
            cycle_start = time.monotonic()
            for location in locations:
                if STOP_EVENT.is_set():
                    break
                try:
                    payload = fetch_response(
                        location=location,
                        api_key=api_key,
                        budget=budget,
                        endpoint=endpoint,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                    )
                    if payload is None:
                        continue
                    event = normalize_response(
                        location, payload, store_raw_response=store_raw
                    )
                    producer.send(topic, key=location["location_id"], value=event).get(timeout=30)
                    LOG.info(
                        "Published real provider observation: location_id=%s collected_at_utc=%s",
                        location["location_id"], event["collected_at_utc"],
                    )
                except BudgetExceeded as exc:
                    LOG.error("Stopping collection: %s", exc)
                    return
                except ProviderAuthenticationError as exc:
                    LOG.error("Stopping collection: %s", exc)
                    return
                except (ValueError, TypeError, KeyError) as exc:
                    LOG.error(
                        "Provider payload could not be normalized: location_id=%s error_type=%s",
                        location["location_id"], type(exc).__name__,
                    )
            producer.flush(timeout=30)
            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, interval_seconds - elapsed)
            if not STOP_EVENT.is_set() and sleep_for:
                STOP_EVENT.wait(sleep_for)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        LOG.info("Collector stopped")


if __name__ == "__main__":
    main()
