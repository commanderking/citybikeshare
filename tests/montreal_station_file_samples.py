"""Sample Montreal station files, used by test_resolve_montreal_station_names.

Each constant is the exact CSV content of one station file as the BIXI source ships it. They
live here (not inline in the tests) so the tests read as behavior and this file reads as a
catalog of what the real source looks like across eras. See the `─── Data eras ───` block in
config/cities/montreal.yaml for the era definitions.

    era A  2014-2020  Stations_<year>.csv   header: code,name,latitude,longitude
                                             (2019 alone capitalizes it as `Code`)
    era B  2021       2021_stations.csv      header: pk,name,latitude,longitude
    era C  2022+       (no station file — trips already carry the name inline)
"""

# ── era A ───────────────────────────────────────────────────────────────────────────

# 2015: code 6100 is "Peel / Sherbrooke".
STATIONS_2015 = """\
code,name,latitude,longitude
6100,Peel / Sherbrooke,45.5049,-73.5732
6200,Marie-Anne / Saint-Denis,45.5250,-73.5830
"""

# 2016: code 6100 has been REASSIGNED to a different physical station than in 2015 — the
# cross-year code reuse that forces the join to key on the trip's own year.
STATIONS_2016 = """\
code,name,latitude,longitude
6100,Parc La Fontaine / Rachel,45.5266,-73.5695
6200,Marie-Anne / Saint-Denis,45.5250,-73.5830
"""

# 2018: the last year code 6034 is listed (it is dropped from the 2019 file below).
STATIONS_2018 = """\
code,name,latitude,longitude
6034,St-Urbain / René-Lévesque,45.5078,-73.5631
"""

# 2019: the one year whose id header is capitalized (`Code`). It does NOT list 6034 — the
# real roster gap that the last-known-name fallback covers.
STATIONS_2019 = """\
Code,name,latitude,longitude
7001,Métro Jean-Talon (Berri / Jean-Talon),45.5390,-73.6142
"""

# ── era B ───────────────────────────────────────────────────────────────────────────

# 2021: its own id namespace (emplacement_pk, small integers) in a single file.
STATIONS_2021 = """\
pk,name,latitude,longitude
10,Métro Angrignon (Lamont / des Trinitaires),45.4469,-73.6036
13,Métro de l'Église (Ross / de l'Église),45.4627,-73.5660
"""
