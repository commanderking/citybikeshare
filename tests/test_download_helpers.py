from citybikeshare.etl.custom_downloaders.utils.download_helpers import prune_stale_downloads


def test_prunes_only_unlisted_files_matching_pattern(tmp_path):
    for name in ["kept.zip", "stale.zip", "notes.txt"]:
        (tmp_path / name).write_text("x")

    prune_stale_downloads(tmp_path, {"kept.zip"}, "*.zip")

    assert sorted(p.name for p in tmp_path.iterdir()) == ["kept.zip", "notes.txt"]


def test_empty_listing_prunes_nothing(tmp_path):
    (tmp_path / "good.zip").write_text("x")

    prune_stale_downloads(tmp_path, set())

    assert (tmp_path / "good.zip").exists()
