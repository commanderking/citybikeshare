import json
import math

from citybikeshare.context import PipelineContext
from citybikeshare.config.loader import load_city_config

# Stage-2 defaults; any can be overridden under `coordinates.canonicalize` per city.
_DEFAULTS = {
    "merge_radius_m": 5,
    "n_obs_floor": 10,
    # Substrings marking a name as operational / non-primary (test rigs, seasonal or temp
    # sites, vendor labels). Such names are deprioritized when picking a cluster's canonical
    # name, but are still kept as aliases.
    "markers": ["former", "temp", "temporary", "winter", "pbsc", "warehouse", "test"],
}


def _compute_distance_m(lat1, lng1, lat2, lng2) -> float:
    """Local equirectangular approximation — exact enough at the <5 m scale we cluster at."""
    r = 6371000.0
    p = math.pi / 180.0
    return r * math.hypot(
        (lat2 - lat1) * p, (lng2 - lng1) * p * math.cos((lat1 + lat2) / 2 * p)
    )


def _cluster_points(points, radius_m):
    """Union-find over points within ``radius_m``. A grid index keyed by ``radius``-sized
    cells keeps this near-linear instead of O(n^2), so big cities stay fast."""
    parent = list(range(len(points)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    # cell size in degrees ~ radius; a point's neighbors can only be in the 3x3 cells around it
    deg = max(radius_m / 111_000.0, 1e-9)
    grid: dict[tuple[int, int], list[int]] = {}
    for i, (_, lat, lng, *_rest) in enumerate(points):
        grid.setdefault((int(lat / deg), int(lng / deg)), []).append(i)

    for i, (_, lat, lng, *_rest) in enumerate(points):
        ci, cj = int(lat / deg), int(lng / deg)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for j in grid.get((ci + di, cj + dj), ()):
                    if (
                        j > i
                        and _compute_distance_m(lat, lng, points[j][1], points[j][2])
                        < radius_m
                    ):
                        union(i, j)

    clusters: dict[int, list[int]] = {}
    for i in range(len(points)):
        clusters.setdefault(find(i), []).append(i)
    return list(clusters.values())


def canonicalize_station_coords(context: PipelineContext):
    """Stage 2 — collapse as-observed names into one canonical point per physical
    station via coordinate proximity, preserving every original name as an alias.

    Reads ``station_coords.json`` (Stage 1) and writes ``station_coords_canonical.json``:
    one record per station, with every other observed name kept under ``aliases``.
    """
    city = context.city
    config = load_city_config(city)
    cfg = {**_DEFAULTS, **(config.get("coordinates", {}).get("canonicalize") or {})}
    markers = [m.lower() for m in cfg["markers"]]

    src = context.analysis_directory / "station_coords.json"
    if not src.exists():
        print(f"⏭️  {city}: no station_coords.json; run generate_station_coords first")
        return
    observed = json.loads(src.read_text())

    # Id-keyed cities (philadelphia, los_angeles) key each record by station id and
    # carry the human name under `name`; that name is the display label here, the id the key.
    #
    # points: (key, lat, lng, n_obs, first_seen, last_seen, display_name).
    # display_name is the record's `name` when present (id-keyed), else the key itself — so
    # name-keyed cities behave exactly as before (display == key).
    points = [
        (
            key,
            v["lat"],
            v["lng"],
            v["n_obs"],
            v.get("first_seen"),
            v.get("last_seen"),
            v.get("name") or key,
        )
        for key, v in observed.items()
    ]

    def has_marker(name):
        low = name.lower()
        return any(m in low for m in markers)

    is_id_keyed = (config.get("coordinates") or {}).get("key", "name") == "id"

    canonical = []
    for idx in _cluster_points(points, cfg["merge_radius_m"]):
        members = [points[i] for i in idx]

        eligible = [
            member
            for member in members
            if not has_marker(member[6]) and member[3] >= cfg["n_obs_floor"]
        ]
        pool = eligible or members  # fall back to all if every name is "ineligible"
        # canonical = most-recent last_seen; n_obs breaks ties / handles missing dates.
        canon = max(pool, key=lambda member: (member[5] or "", member[3]))

        display = canon[6]
        aliases = sorted({member[6] for member in members if member[6] != display})
        firsts = [member[4] for member in members if member[4]]
        lasts = [member[5] for member in members if member[5]]
        record = {
            "name": display,
            "lat": canon[1],
            "lng": canon[2],
            "first_seen": min(firsts) if firsts else None,
            "last_seen": max(lasts) if lasts else None,
            "aliases": aliases,
        }
        # For id-keyed cities the key isn't the display name — keep the station id(s) that
        # merged into this point so the record still joins back to the id-keyed trip data.
        if is_id_keyed:
            record["ids"] = sorted({member[0] for member in members})
        canonical.append(record)

    canonical.sort(key=lambda record: record["name"])

    out_canon = context.analysis_directory / "station_coords_canonical.json"
    with open(out_canon, "w") as f:
        json.dump(canonical, f, indent=2, ensure_ascii=False)

    merged = sum(len(record["aliases"]) for record in canonical)
    print(
        f"✅ {city}: {len(observed)} observed → {len(canonical)} canonical "
        f"({merged} names merged)\n   → {out_canon}"
    )
