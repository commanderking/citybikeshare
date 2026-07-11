# TODO: unify how cities declare their coordinate source

## Goal

Cities drive their station coordinates in several different ways, and that config is currently
smeared across four places in each city's YAML. Long-term we want **one area of the YAML** that
declares *where a city's coordinates come from*, with the fetch / refresh / read / name-join all
derived from it.

## Why (the source-type variety pushing this)

| Source shape | Cities | Where coords live |
| --- | --- | --- |
| inline in trip rows | chattanooga (WKT point), daejeon (lat/lng), boston, nyc, … | trip CSV columns |
| local station file | guadalajara (`nomenclatura_*.csv`) | file shipped with the data |
| GBFS feed | mexico_city, vancouver, toronto, helsinki(? dead), pittsburgh | `station_information.json` |
| bespoke API / open-data | london (TfL BikePoint), austin (Socrata), seoul, taipei (YouBike), rosario | remote endpoint |

## The current fragmentation (what a "source" actually touches)

| Concern | Stage | Config today |
| --- | --- | --- |
| where points come from / how to read them | analyze | `coordinates:` (`strategy`, `format`, `station_file`, `key`) |
| how to fetch a remote source | sync | `gbfs_url:` (top-level) |
| merge into committed reference | transform | `pre_transform_pipeline: [refresh_station_coordinates]` |
| resolve id→name for id-only trips | transform | `processing_pipeline: [join_mexico_city_station_names / handle_guadalajara_stations]` |

A coordinate source drives all four; they should be declared once.

---

## Option 1 — Consolidate into `coordinates.source` (DOING NOW)

Fold `gbfs_url` + the refresh trigger into the `coordinates:` block; keep the existing
`strategy`/`station_file` read path underneath.

```yaml
coordinates:
  source: { type: gbfs, url: "https://…/gbfs.json" }   # fetch (sync) + auto-refresh (transform)
  strategy: station_file
  key: name
  bounding_box: [...]
  min_coverage: 0.85
  station_file: { format: station_coordinates, id: [id], name: [name], lat: [lat], lng: [lng] }
```

- Sync fetches when `source.type: gbfs`; transform auto-refreshes the committed table — **no
  separate `gbfs_url` / `pre_transform` entry to keep in sync** (fixes the Vancouver half-wired
  footgun).
- **Known remaining redundancy:** `source` (fetch: gbfs) and `station_file`/`strategy` (read:
  committed table) still coexist — Option 1 groups the *fetch+refresh*, not the read. Options 2/3
  collapse that.

## Option 2 — Split `stations:` (source) from `coordinates:` (tuning)

One block owns the source of truth, consulted by sync + transform + analyze; `coordinates:` keeps
only analysis knobs.

```yaml
stations:
  source: gbfs                 # inline | file | gbfs | api
  url: "https://…/gbfs.json"
  columns: { id, name, lat, lng }
  key: name
  provides: [names, coordinates]     # names → a generic resolve_station_names replaces the
                                     # city-specific join_mexico_city_station_names / handle_guadalajara_stations

coordinates:
  bounding_box: [...]
  min_coverage: 0.85
  canonicalize: { n_obs_floor: 0 }
```

- Highest-leverage single step: `provides: [names]` retires the last city-specific coordinate
  code (the transform name-join).
- Tradeoff: two blocks whose "source vs tuning" boundary people will occasionally fumble
  (e.g. `bounding_box` arguably belongs to the source).

## Option 3 — Coordinate-**provider** registry (endgame)

Each source type is a provider with a uniform lifecycle: `fetch(ctx)` → `refresh(ctx)` (persist
committed table) → `read(ctx) → [id,name,lat,lng]` → optional `names()`. Inline/local providers
no-op fetch/refresh. YAML just names the provider:

```yaml
coordinates:
  provider: gbfs               # inline_latlng | inline_wkt | local_csv | gbfs | socrata | tfl_bikepoint | youbike
  params: { url: "…", columns: {…} }
  key: name
  bounding_box: [...]
```

- Unifies today's `COORD_FORMATS` + `STATION_FILE_READERS` into one provider registry spanning all
  stages. Natural home for TfL / Socrata / YouBike / POGOH / rosario as one-line additions.
- **Red-team (important):** the uniform interface unifies *dispatch + lifecycle*, but each remote
  source still needs *bespoke fetch/parse* code (TfL API, Socrata query, YouBike dataset, HSL's
  broken discovery doc all differ). The "add a source = one line" promise only covers the wiring;
  you still write a thin per-source reader. The win is a single seam instead of four.

## Recommendation / sequencing

1. **Option 1 now** (small; fixes the footgun independently). ← in progress
2. Target **Option 3's provider seam**, reached incrementally: `read()` ≈ today's
   `STATION_FILE_READERS` + `COORD_FORMATS` merged; `fetch()` ≈ `download_station_information`
   generalized. **Option 2's `provides:[names]`** generalization of the transform join is the
   highest-value single step.
3. **No big-bang rewrite.** ~6 remaining cities need a bespoke fetch/parse regardless of config
   shape, so unification buys readability + removes footguns, not free cities. Land the seam once,
   then each new city = "write the thin reader + name the provider."
