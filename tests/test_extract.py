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

        assert (
            _orphan_raw_files(raw, previous_run_metadata={}, current_run_metadata={})
            == {}
        )
        assert (raw / "OD_2014-04.csv").exists()

    def test_ignores_recorded_output_already_gone(self, tmp_path):
        """A recorded output whose file was already removed isn't flagged."""
        raw = tmp_path / "raw"
        raw.mkdir()

        previous = {"old.zip": {"outputs": ["already_deleted.csv"]}}
        assert _orphan_raw_files(raw, previous, current_run_metadata={}) == {}


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


class TestExtractTxtMembers:
    def test_stores_txt_bytes_verbatim_as_csv_gz(self, tmp_path):
        import gzip
        import zipfile
        from citybikeshare.etl.extract import _extract_archive

        # Latin-1, tab-delimited — must survive extract byte-for-byte (clean re-encodes).
        payload = "Id\tOrigen\r\n1\tPlaza San Mart\xedn\r\n".encode("latin1")
        zip_path = tmp_path / "src.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("Mi bici tu bici 2021.txt", payload)
            zf.writestr("README.txt", "about this data")
        raw = tmp_path / "raw"
        raw.mkdir()

        raw_files = _extract_archive(zip_path, raw, "src.zip", {}, {})

        assert [p.name for p in raw_files] == ["Mi bici tu bici 2021.csv.gz"]
        assert gzip.decompress(raw_files[0].read_bytes()) == payload
        assert not (raw / "README.csv.gz").exists()


def _zip_with(path, member, body):
    import zipfile

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(member, body)
    return path


class TestRawNameCollisions:
    """Capital Bikeshare's April 2024 zip holds `202004-capitalbikeshare-tripdata.csv`."""

    APRIL_2020 = "202004-capitalbikeshare-tripdata.csv"

    def test_raises_before_overwriting_another_downloads_raw_file(self, tmp_path):
        import gzip
        import pytest
        from citybikeshare.etl.extract import _extract_archive

        raw = tmp_path / "raw"
        raw.mkdir()
        download_by_raw_file: dict = {}
        z2020 = _zip_with(tmp_path / "202004.zip", self.APRIL_2020, b"2020 trips\n")
        z2024 = _zip_with(
            tmp_path / "202404.zip", self.APRIL_2020, b"2024 trips, more\n"
        )

        _extract_archive(z2020, raw, "202004.zip", download_by_raw_file, {})
        with pytest.raises(ValueError, match="renamed_archive_members"):
            _extract_archive(z2024, raw, "202404.zip", download_by_raw_file, {})

        stored = gzip.decompress((raw / (self.APRIL_2020 + ".gz")).read_bytes())
        assert stored == b"2020 trips\n"

    def test_identical_rebundle_from_another_download_is_kept(self, tmp_path):
        """London: a loose extract and the same file inside the yearly zip."""
        from citybikeshare.etl.extract import _extract_archive

        raw = tmp_path / "raw"
        raw.mkdir()
        download_by_raw_file: dict = {}
        loose = _zip_with(tmp_path / "loose.zip", "01a.csv", b"same\n")
        yearly = _zip_with(tmp_path / "2016.zip", "01a.csv", b"same\n")

        _extract_archive(loose, raw, "loose.zip", download_by_raw_file, {})
        raw_files = _extract_archive(yearly, raw, "2016.zip", download_by_raw_file, {})

        assert [p.name for p in raw_files] == ["01a.csv.gz"]

    def test_renamed_member_gets_its_own_raw_file(self, tmp_path):
        from citybikeshare.etl.extract import _extract_archive

        raw = tmp_path / "raw"
        raw.mkdir()
        download_by_raw_file: dict = {}
        z2020 = _zip_with(tmp_path / "202004.zip", self.APRIL_2020, b"2020 trips\n")
        z2024 = _zip_with(
            tmp_path / "202404.zip", self.APRIL_2020, b"2024 trips, more\n"
        )
        renames = {self.APRIL_2020: "202404-capitalbikeshare-tripdata.csv"}

        _extract_archive(z2020, raw, "202004.zip", download_by_raw_file, {})
        raw_files = _extract_archive(
            z2024, raw, "202404.zip", download_by_raw_file, renames
        )

        assert [p.name for p in raw_files] == [
            "202404-capitalbikeshare-tripdata.csv.gz"
        ]
        assert (raw / (self.APRIL_2020 + ".gz")).exists()

    def test_download_gone_from_download_dir_is_dropped(self):
        """Daejeon: a renamed cumulative archive may rewrite its predecessor's member."""
        from citybikeshare.etl.extract import _map_raw_files_to_downloads

        state = {
            "bundle_2025_01.zip": {"outputs": ["trips.csv.gz"]},
            "other.zip": {"outputs": ["other.csv.gz"]},
        }

        download_by_raw_file = _map_raw_files_to_downloads(
            state, {"bundle_2025_0102.zip", "other.zip"}
        )

        assert download_by_raw_file == {"other.csv.gz": "other.zip"}
