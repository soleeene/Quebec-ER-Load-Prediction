# Quebec-ER-Load-Prediction
Group project for the Data Lakes and Data Warehouse project.

# Research questions possibilities
- How does traffic and traffic incidents impact ER load in hospitals of Montréal and Québec ? Do the incidents types have an impact on the ER load ?
- How does the weather and the weekday/holiday impact ER load in hospitals of Montréeal and Québec ?
- Are there significant differences in hospitals ER loads within the same city at the same moment ? Can this difference be explained by the hospital ER capacity (hospital size) ? Or by the hospital's proximity with the current traffic incidents ?
- What are the daily (and seasonal) ER load patterns of hospitals in Québec ?
- Is it possible to predict with an accuray of 90% the overall ER load in the hospitals of Montréal and Québec for the next 1 to 24h given historical data, weather data, traffic data and calendar data ?

-> We have to keep in mind that we will at most have a few months of data. Some answers we find might be incomplete / not very precise.

# Accessible data 

## Québec ER load data
[API doc](https://sante.handled.tools/en/donnees)

### ER Load Ingestion

Hourly Lambda that pulls Quebec ER data from `sante.handled.tools` (MSSS open data, CC-BY 4.0) and writes it to S3.

### Outputs

All under `s3://$S3_BUCKET/$S3_PREFIX/` (default prefix `er-load`):

| Path | Format | Content | Written |
|---|---|---|---|
| `facilities/facilities.json` | JSON array | Facility directory | Every run (overwritten) |
| `now/year=YYYY/month=MM/day=DD/hour=HH/now.jsonl.gz` | gzipped JSONL | Current status of all ERs | Every run (one file per hour) |
| `history/facility_id={id}/history.jsonl.gz` | gzipped JSONL | Full history per facility | Daily at `HISTORY_HOUR_UTC` (default 03h), or when missing for a facility |

### Schemas

**facilities** (one object per facility, ~120)

| Field | Type | Description |
|---|---|---|
| `id` | string | Facility ID (join key) |
| `name` | string | Facility name |
| `establishment` | string | Parent health establishment |
| `region_code` | string | Region code (`"01"`…) |
| `region_name` | string | Region name |
| `address` | string | Street address, city, postal code |
| `lat`, `lng` | float | Coordinates |
| `phone` | string \| null | Always null in current data |
| `slug` | string | URL slug |

**now** (one row per ER, per hourly snapshot)

| Field | Type | Description |
|---|---|---|
| `_fetched_at` | string | Snapshot time, UTC ISO 8601 (added by the Lambda) |

**now** (one row per ER, per hourly snapshot)

| Field | Type | Description |
|---|---|---|
| `id` | string | Facility ID (joins to `facilities.id`, `history.facility_id`) |
| `name` | string | Facility name |
| `establishment` | string | Parent health establishment |
| `regionCode` | string | Region code |
| `region` | string | Region name |
| `civFonct` | int | Functional stretchers (capacity) |
| `civOcc` | int | Occupied stretchers |
| `occPct` | int | Stretcher occupancy in % (`civOcc / civFonct`) |
| `p24h` | int | Patients on stretcher > 24 h (likely) |
| `p48h` | int | Patients on stretcher > 48 h (likely) |
| `present` | int | Patients currently in the ER |
| `waiting` | int | Patients waiting |
| `dmsCiviereMin` | int | Average stay on stretcher, minutes (likely) |
| `dmsAmbMin` | int | Average stay of ambulatory patients, minutes (likely) |
| `ts` | string | Hour the MSSS data refers to, e.g. `2026-10-28T11:00` (no timezone) |
| `docWaitMin` | int | Wait to see a doctor, minutes (likely) |
| `docWaitingDoc` | int | Patients waiting to see a doctor (likely) |
| `docWaitAt` | string | Timestamp of the doctor-wait figure |
| `_fetched_at` | string | Fetch time, UTC ISO 8601 (added by the Lambda) |

**history** (one row per facility per hour)

| Field | Type | Description |
|---|---|---|
| `ts` | string | Hour, e.g. `2026-10-21T17:00` (no timezone) |
| `occ_pct` | int | ER occupancy in % (can exceed 100) |
| `waiting` | int | Patients waiting |
| `present` | int | Patients present |
| `facility_id` | string | Facility ID (added if missing) |

### Return value

```json
{"fetched_at": "...", "facilities": 0, "now_rows": 0,
 "history_ok": 0, "history_failed": [], "history_rows": 0}
```

### Config

`S3_BUCKET` (required), `S3_PREFIX`, `HISTORY_HOUR_UTC`, `FORCE_HISTORY`, `MAX_WORKERS`

This service needs no authentification. The data is json-like (can be converted into (lists of) dictionnaries).

## Traffic data
[API doc](https://docs.tomtom.com/traffic-api/api-explorer/tomtom-orbis-maps-v2/traffic-orbis-v2)
Tomtom's API enables to gather data from traffic, like absolute and relative driving speed on each road, the road category, the road importance ranking, the road's coordinates ([doc](https://docs.tomtom.com/traffic-api/documentation/tomtom-orbis-maps/v1/traffic-flow/vector-flow-tiles)). Another endpoint offers data about traffic incidents, like accident type, roadworks, lane closures, delay in seconds, average speed within the incident, number of user reports, incident descriptions ([doc](https://docs.tomtom.com/traffic-api/documentation/tomtom-orbis-maps/v2/traffic-incidents/vector-incident-tiles)).
The data is either geoJSON or JSON.

This service needs authentification.

In case this service is not satisfactory enough, the use of the HERE platform could be invastigated [doc](https://docs.here.com/traffic-api/docs/introduction-to-here-traffic-api-v7).

## Meteorological data
[API doc](https://open-meteo.com/en/docs)
Open-meteo is a service that offers meteo data given a latitude and longitude. One endpoint offers current data and another one offers historical data. Many measurements are retrievable (tmeperature, precipitations, snowfall, windspeed, humidity, ...).
This service needs no authentification.

## Calendar data
[API doc](https://www.abstractapi.com/api/holidays-api)
APi offering data about public holidays and weekday.
This service needs authentification.
