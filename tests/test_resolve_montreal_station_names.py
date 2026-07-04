"""Unit tests for resolve_montreal_station_names.

Montreal's open data comes in three eras (see the `─── Data eras ───` block in
config/cities/montreal.yaml). The older eras identify a station by a numeric key only; the
human station name lives in separate per-year station files, and this step attaches it:
"""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from citybikeshare.context import PipelineContext
from citybikeshare.etl.pipelines.common import resolve_montreal_station_names
from tests import montreal_station_file_samples as samples


def _montreal_context(tmp_path: Path) -> PipelineContext:
    ctx = PipelineContext(
        city="montreal",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.raw_directory.mkdir(parents=True, exist_ok=True)
    return ctx


def _write_station_file(ctx: PipelineContext, filename: str, contents: str) -> None:
    """Drop one of the sample station files into raw/ under its source filename."""
    (ctx.raw_directory / filename).write_text(contents, encoding="utf-8")


def _run(trips: pl.LazyFrame, config: dict, ctx: PipelineContext) -> pl.DataFrame:
    return resolve_montreal_station_names(trips, config, ctx).collect()


def test_era_a_maps_code_to_name_using_the_trips_own_year(tmp_path):
    """era A: the name comes from the trip's OWN year's file — because Montreal reuses a
    code for a different physical station over time (6100 below), so the same code must
    resolve differently by year (a latest-wins union would mislabel the old trips)."""
    ctx = _montreal_context(tmp_path)
    _write_station_file(ctx, "Stations_2015.csv", samples.STATIONS_2015)
    _write_station_file(ctx, "Stations_2016.csv", samples.STATIONS_2016)
    config = {
        "station_files": {
            "code_years": {2015: "Stations_2015.csv", 2016: "Stations_2016.csv"}
        }
    }

    trips = pl.LazyFrame(
        {
            # Only start_time's YEAR drives the join (it keys both endpoints).
            "start_time": [datetime(2015, 7, 1, 8, 0), datetime(2016, 7, 1, 8, 0)],
            "start_station_code": ["6100", "6100"],
            "end_station_code": ["6200", "6100"],
        }
    )
    out = _run(trips, config, ctx).sort("start_time")

    # 2015 trip: 6100 -> the 2015 name.
    assert out["start_station_name"][0] == "Peel / Sherbrooke"
    assert out["end_station_name"][0] == "Marie-Anne / Saint-Denis"
    # 2016 trip: the SAME code 6100 -> the 2016 name.
    assert out["start_station_name"][1] == "Parc La Fontaine / Rachel"
    assert out["end_station_name"][1] == "Parc La Fontaine / Rachel"


def test_era_a_tolerates_the_2019_capitalized_Code_header(tmp_path):
    """Every year's file uses the header `code` — except 2019, which capitalizes it as
    `Code`. The reader accepts whichever is present."""
    ctx = _montreal_context(tmp_path)
    _write_station_file(ctx, "Stations_2019.csv", samples.STATIONS_2019)
    config = {"station_files": {"code_years": {2019: "Stations_2019.csv"}}}

    trips = pl.LazyFrame(
        {
            "start_time": [datetime(2019, 5, 1, 9, 0)],
            "start_station_code": ["7001"],
            "end_station_code": ["7001"],
        }
    )
    out = _run(trips, config, ctx)

    assert out["start_station_name"][0] == "Métro Jean-Talon (Berri / Jean-Talon)"


def test_era_a_falls_back_to_another_year_when_code_missing_from_its_own_file(tmp_path):
    """A few codes are absent from their own year's file (a gap in that roster). Rather than
    drop the name, the join falls back to last_known_name_map — the most recent name any year
    gave the code. This is exactly the real 2026-07 case: the only two codes that need it are
    6034 (St-Urbain / René-Lévesque, listed through 2018) and 6708, both missing from
    Stations_2019.csv yet still referenced by 2019 trips. This test reproduces 6034."""
    ctx = _montreal_context(tmp_path)
    _write_station_file(ctx, "Stations_2018.csv", samples.STATIONS_2018)
    _write_station_file(ctx, "Stations_2019.csv", samples.STATIONS_2019)  # lacks 6034
    config = {
        "station_files": {
            "code_years": {2018: "Stations_2018.csv", 2019: "Stations_2019.csv"}
        }
    }

    trips = pl.LazyFrame(
        {
            "start_time": [datetime(2019, 6, 1, 8, 0)],
            "start_station_code": ["6034"],  # no (2019, 6034) -> last_known_name_map
            "end_station_code": ["7001"],  # exact (2019, 7001) hit
        }
    )
    out = _run(trips, config, ctx)

    assert out["start_station_name"][0] == "St-Urbain / René-Lévesque"
    assert out["end_station_name"][0] == "Métro Jean-Talon (Berri / Jean-Talon)"


def test_era_a_raises_when_a_code_maps_to_no_station_file(tmp_path):
    """Loud safety net: a code present in the trips but in NO station file would otherwise
    become a silent null. Instead the step raises, so a roster that stops covering a code
    (e.g. a new year's file was never added) surfaces immediately instead of shipping
    nameless trips."""
    ctx = _montreal_context(tmp_path)
    _write_station_file(ctx, "Stations_2016.csv", samples.STATIONS_2016)
    config = {"station_files": {"code_years": {2016: "Stations_2016.csv"}}}

    trips = pl.LazyFrame(
        {
            "start_time": [datetime(2016, 7, 1, 8, 0)],
            "start_station_code": ["9999"],  # exists in no station file
            "end_station_code": ["6100"],
        }
    )
    with pytest.raises(ValueError, match="no station-file match"):
        _run(trips, config, ctx)


def test_era_b_2021_maps_emplacement_pk_to_name(tmp_path):
    """era B (2021): stations are keyed by emplacement_pk in their own id namespace, with a
    single file (2021_stations.csv, header `pk`) — no per-year reuse to worry about."""
    ctx = _montreal_context(tmp_path)
    _write_station_file(ctx, "2021_stations.csv", samples.STATIONS_2021)
    config = {"station_files": {"pk_file": "2021_stations.csv"}}

    trips = pl.LazyFrame(
        {
            "start_time": [datetime(2021, 8, 1, 10, 0)],
            "emplacement_pk_start": ["10"],
            "emplacement_pk_end": ["13"],
        }
    )
    out = _run(trips, config, ctx)

    assert out["start_station_name"][0] == "Métro Angrignon (Lamont / des Trinitaires)"
    assert out["end_station_name"][0] == "Métro de l'Église (Ross / de l'Église)"


def test_era_c_2022_plus_passes_inline_names_through_untouched(tmp_path):
    """era C (2022+): trips already carry real station names inline, so the step is a no-op
    and must not read the station files at all. We prove it by pointing the config at files
    that do not exist — era C never touches them."""
    ctx = _montreal_context(tmp_path)
    config = {
        "station_files": {
            "code_years": {2016: "does_not_exist.csv"},
            "pk_file": "missing.csv",
        }
    }

    trips = pl.LazyFrame(
        {
            "start_time": [datetime(2026, 2, 1, 7, 0)],
            "start_station_name": ["Métro Mont-Royal (Place Gérald-Godin)"],
            "end_station_name": ["Calixa-Lavallée / Rachel"],
        }
    )
    out = _run(trips, config, ctx)

    assert out["start_station_name"][0] == "Métro Mont-Royal (Place Gérald-Godin)"
    assert out["end_station_name"][0] == "Calixa-Lavallée / Rachel"
