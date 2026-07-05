from pathlib import Path

import polars as pl
import pytest

from citybikeshare.context import PipelineContext
from citybikeshare.etl import station_maps
from citybikeshare.etl.pipelines.common import handle_guadalajara_stations
from citybikeshare.etl.transform import run_pre_transform_steps


def _context(tmp_path: Path) -> PipelineContext:
    ctx = PipelineContext(
        city="guadalajara",
        data_root=tmp_path / "data",
        transformed_root=tmp_path / "output",
        analysis_root=tmp_path / "analysis",
    )
    ctx.download_directory.mkdir(parents=True, exist_ok=True)
    ctx.raw_directory.mkdir(parents=True, exist_ok=True)
    return ctx


@pytest.fixture
def isolated_map(tmp_path, monkeypatch):
    """Point the committed-map path at a tmp dir so tests don't touch the repo file."""
    monkeypatch.setattr(station_maps, "STATION_MAP_DIR", tmp_path / "station_maps")
    return station_maps.station_map_path("guadalajara")


def _write_nomenclatura(directory: Path, month: str, rows: list[tuple[int, str]]):
    lines = ["id,name"] + [f"{sid},{name}" for sid, name in rows]
    (directory / f"nomenclatura_{month}.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


class TestUpdateGuadalajaraStationMap:
    def test_newest_wins_and_old_ids_retained(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        # 10 renamed between months; 401 exists only in the older file.
        _write_nomenclatura(
            ctx.download_directory, "2025_10", [(10, "OLD NAME"), (401, "ZPN-079")]
        )
        _write_nomenclatura(
            ctx.download_directory, "2026_05", [(10, "NEW NAME"), (4, "Niños héroes")]
        )

        station_maps.update_guadalajara_station_map(ctx)

        df = pl.read_csv(isolated_map, infer_schema_length=0)
        mapping = dict(zip(df["id"].to_list(), df["name"].to_list()))
        assert mapping["10"] == "NEW NAME"  # newest file wins
        assert mapping["401"] == "ZPN-079"  # id only in the old file is retained
        assert mapping["4"] == "Niños héroes"  # clean UTF-8 preserved
        # sorted numerically by id
        assert df["id"].cast(pl.Int64).to_list() == sorted(
            df["id"].cast(pl.Int64).to_list()
        )

    def test_idempotent_no_rewrite(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(ctx.download_directory, "2026_05", [(2, "A"), (3, "B")])
        station_maps.update_guadalajara_station_map(ctx)
        before = isolated_map.read_bytes()
        station_maps.update_guadalajara_station_map(ctx)
        assert isolated_map.read_bytes() == before

    def test_raises_without_source_files(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        with pytest.raises(FileNotFoundError):
            station_maps.update_guadalajara_station_map(ctx)


class TestPreTransformSteps:
    STEP = "refresh_guadalajara_station_map"

    def test_noop_when_not_configured(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        # no pre_transform_pipeline → nothing runs, nothing written
        run_pre_transform_steps(ctx, {})
        assert not isolated_map.exists()

    def test_best_effort_skips_without_nomenclatura(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        # configured but no station files present → keep committed map, don't raise
        run_pre_transform_steps(ctx, {"pre_transform_pipeline": [self.STEP]})
        assert not isolated_map.exists()

    def test_refreshes_when_configured_and_files_present(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(ctx.download_directory, "2026_05", [(2, "A")])
        run_pre_transform_steps(ctx, {"pre_transform_pipeline": [self.STEP]})
        assert isolated_map.exists()
        assert pl.read_csv(isolated_map, infer_schema_length=0)["id"].to_list() == ["2"]

    def test_unknown_step_raises(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        with pytest.raises(KeyError):
            run_pre_transform_steps(ctx, {"pre_transform_pipeline": ["nope"]})


class TestHandleGuadalajaraStations:
    def test_joins_names_for_covered_ids(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(
            ctx.download_directory, "2026_05", [(2, "Station Two"), (4, "Station Four")]
        )
        station_maps.update_guadalajara_station_map(ctx)

        trips = pl.LazyFrame({"start_station_id": [2], "end_station_id": [4]})
        out = handle_guadalajara_stations(trips, {}, ctx).collect()
        assert out["start_station_name"].to_list() == ["Station Two"]
        assert out["end_station_name"].to_list() == ["Station Four"]

    def test_raises_loud_on_missing_station(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(ctx.download_directory, "2026_05", [(2, "Station Two")])
        station_maps.update_guadalajara_station_map(ctx)

        trips = pl.LazyFrame({"start_station_id": [2], "end_station_id": [999]})
        with pytest.raises(ValueError, match="999"):
            handle_guadalajara_stations(trips, {}, ctx).collect()

    def test_allowlisted_id_passes_with_fallback_name(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(ctx.download_directory, "2026_05", [(2, "Station Two")])
        station_maps.update_guadalajara_station_map(ctx)

        trips = pl.LazyFrame({"start_station_id": [2], "end_station_id": [408]})
        config = {"unmapped_station_ids": [408]}
        out = handle_guadalajara_stations(trips, config, ctx).collect()
        assert out["start_station_name"].to_list() == ["Station Two"]
        assert out["end_station_name"].to_list() == ["Unknown (id 408)"]

    def test_non_allowlisted_missing_still_raises(self, tmp_path, isolated_map):
        ctx = _context(tmp_path)
        _write_nomenclatura(ctx.download_directory, "2026_05", [(2, "Station Two")])
        station_maps.update_guadalajara_station_map(ctx)

        trips = pl.LazyFrame({"start_station_id": [2], "end_station_id": [999]})
        config = {"unmapped_station_ids": [408]}  # 999 not exempt
        with pytest.raises(ValueError, match="999"):
            handle_guadalajara_stations(trips, config, ctx).collect()

    def test_raises_when_allowlisted_id_is_now_named(self, tmp_path, isolated_map):
        # a later nomenclatura names 408, so the stale allow-list entry must be pruned
        ctx = _context(tmp_path)
        _write_nomenclatura(
            ctx.download_directory, "2026_08", [(2, "Station Two"), (408, "Named Now")]
        )
        station_maps.update_guadalajara_station_map(ctx)

        trips = pl.LazyFrame({"start_station_id": [2], "end_station_id": [408]})
        config = {"unmapped_station_ids": [408]}
        with pytest.raises(ValueError, match="now named"):
            handle_guadalajara_stations(trips, config, ctx).collect()
