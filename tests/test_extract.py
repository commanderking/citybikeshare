from citybikeshare.etl.extract import _apply_orphan_cleanup, _orphan_raw_files


def _touch(path):
    path.write_text("x")


class TestOrphanRawFiles:
    def test_detects_output_of_vanished_archive(self, tmp_path):
        raw = tmp_path / "raw"
        raw.mkdir()
        _touch(raw / "DonneesOuvertes2026_010203.csv")

        previous = {
            "DonneesOuvertes2026_010203.zip": {
                "outputs": ["DonneesOuvertes2026_010203.csv"]
            }
        }
        # The renamed archive is what's present this run.
        new = {
            "DonneesOuvertes2026_01020304.zip": {
                "outputs": ["DonneesOuvertes2026_01020304.csv"]
            }
        }

        orphans = _orphan_raw_files(raw, previous, new)

        assert orphans == {
            "DonneesOuvertes2026_010203.zip": ["DonneesOuvertes2026_010203.csv"]
        }

    def test_ignores_output_still_produced_by_present_archive(self, tmp_path):
        """Cumulative archive with a stable member name (e.g. Daejeon): the zip
        was renamed, but the member it emits is unchanged, so it isn't an orphan."""
        raw = tmp_path / "raw"
        raw.mkdir()
        _touch(raw / "trips.csv")

        previous = {"bundle_2025_01.zip": {"outputs": ["trips.csv"]}}
        new = {"bundle_2025_0102.zip": {"outputs": ["trips.csv"]}}

        assert _orphan_raw_files(raw, previous, new) == {}
        assert (raw / "trips.csv").exists()

    def test_ignores_untracked_legacy_files(self, tmp_path):
        """Files never recorded in prior state (e.g. legacy OD_2014-04.csv) are
        never flagged, even when no archive produces them."""
        raw = tmp_path / "raw"
        raw.mkdir()
        _touch(raw / "OD_2014-04.csv")

        assert _orphan_raw_files(raw, previous_state={}, new_state={}) == {}
        assert (raw / "OD_2014-04.csv").exists()

    def test_ignores_recorded_output_already_gone(self, tmp_path):
        """A recorded output whose file was already removed isn't flagged."""
        raw = tmp_path / "raw"
        raw.mkdir()

        previous = {"old.zip": {"outputs": ["already_deleted.csv"]}}
        assert _orphan_raw_files(raw, previous, new_state={}) == {}


class TestApplyOrphanCleanup:
    def test_prune_deletes_files(self, tmp_path):
        raw = tmp_path / "raw"
        raw.mkdir()
        _touch(raw / "old.csv")
        orphans = {"old.zip": ["old.csv"]}

        removed = _apply_orphan_cleanup(raw, orphans, prune=True)

        assert removed == ["old.csv"]
        assert not (raw / "old.csv").exists()

    def test_warn_only_keeps_files(self, tmp_path, capsys):
        raw = tmp_path / "raw"
        raw.mkdir()
        _touch(raw / "old.csv")
        orphans = {"old.zip": ["old.csv"]}

        removed = _apply_orphan_cleanup(raw, orphans, prune=False)

        assert removed == []
        assert (raw / "old.csv").exists()
        out = capsys.readouterr().out
        assert "may now be duplicates" in out
        assert "prune_renamed_archives" in out
