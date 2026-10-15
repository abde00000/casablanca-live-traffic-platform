# Project Plan — Casablanca Live Traffic Intelligence Platform

## 1. Working title

**A Real-Time Big-Data Platform for Monitoring Traffic Congestion in Casablanca**

## 2. Research question

How do provider-observed traffic speeds and travel-time delays vary by hour and across a sample of Casablanca's arrondissements?

## 3. Scope

- Geographic scope: 16 Casablanca arrondissements represented by one configured road point per arrondissement.
- Collection scope: one TomTom Traffic Flow Segment request per point per hour by default.
- Observation period: begins when the API collector is configured and launched. No historical observations are fabricated or backfilled.
- Out of scope: citywide census of all roads, public-transport punctuality, individual vehicle tracking, and official municipal performance claims.

## 4. Data and provenance

### Primary traffic source

TomTom Traffic Flow Segment Data API. Each successful record retains the provider, product, collection timestamp, location ID, source OSM IDs, provider confidence, current/free-flow speeds and travel times, road class, closure status, and an optional provider segment reference. The API response is an estimate derived by the provider; it is not a sensor installed by the student.

### Location reference

The CSV location catalogue contains named OSM road points and OSM relation IDs for arrondissements. The listed coordinates were selected from the closest named major-road way to each OSM arrondissement relation center and queried on 2026-10-15. OSM IDs are retained so that locations can be rechecked. Attribution: © OpenStreetMap contributors, ODbL.

### Authentication and terms

The TomTom API key is supplied locally through `.env`. It must not be committed or embedded in Kafka records. Before retaining a provider response as a long-running historical archive, sharing raw output, or publishing data extracts, the project owner must verify the current developer terms and pricing applicable to their account.

## 5. Sampling and request budget

Default: 16 points × 24 hourly polls × 30 days = 11,520 requests per 30-day month before retries. The collector's local cap is 18,000 requests/month by default. TomTom's published pricing page currently lists 20,000 monthly requests for Flow API Segment Data, but account limits and terms can change; the user must verify before deployment. Keep the local cap below the account limit.

## 6. Data contract and measures

For each successful response:

- `current_speed_kmh` and `free_flow_speed_kmh` — provider-reported road-segment speeds.
- `current_travel_time_s` and `free_flow_travel_time_s` — provider-reported travel times.
- `speed_ratio = current_speed_kmh / free_flow_speed_kmh` when free-flow speed is positive.
- `travel_time_index = current_travel_time_s / free_flow_travel_time_s` when free-flow time is positive.
- `delay_seconds = current_travel_time_s - free_flow_travel_time_s`.
- `delay_pct = 100 × (travel_time_index − 1)`.
- `confidence` — provider-supplied confidence, retained as a quality indicator.
- `collected_at_utc` — request collection time, assigned by the collector.

The Gold job labels delay bands as follows: `LOW_DELAY` for index ≤ 1.10, `MODERATE_DELAY` for > 1.10–1.25, `HIGH_DELAY` for > 1.25–1.50, and `VERY_HIGH_DELAY` for > 1.50. These are analyst-defined display thresholds, not official traffic classifications. The main analysis should show continuous measures and provider confidence alongside any bands.

## 7. Processing design

1. **Ingestion:** Python calls the real API at configured points, applies timeouts/retries/budget control, and sends each successful response to Kafka.
2. **Bronze:** append-only Delta landing stream preserves each Kafka value and broker metadata.
3. **Silver:** parse and validate required fields and types; send malformed/out-of-range records to a reject Delta path with reasons; compute metrics; deduplicate event IDs within a two-hour watermark.
4. **Gold:** append hourly aggregates by location/arrondissement; preserve observation count, confidence, speed, and delay measures.
5. **SQL:** register Bronze/Silver/Gold Delta locations in Hive Metastore and expose analyst-facing views.

## 8. Evaluation questions

- Which sample points show the highest mean and peak-hour travel-time index?
- How does delay vary across hours and weekdays/weekends during the collection period?
- Do provider confidence and road class vary across observed congestion levels?
- How many observations were collected, rejected, or unavailable at each point?
- How stable are results if low-confidence records are excluded?

No results or performance metrics should be written into the report until the real collector has run and the resulting data has been inspected.

## 9. Limitations and responsible interpretation

- One point per arrondissement is a sample, not full road-network coverage; do not generalize it to the whole arrondissement without additional sampling.
- The provider may not have live probe-supported values at every point; confidence must be retained and low-confidence periods examined.
- `collected_at_utc` records query time, not a guaranteed probe timestamp.
- Free-flow values are provider-defined baselines, not necessarily legal speed limits or independently measured free-flow conditions.
- No personal vehicle identifiers, consumer locations, or synthetic traffic events are collected.
- Data retention, display, and redistribution must comply with the API agreement and applicable OSM ODbL obligations.
