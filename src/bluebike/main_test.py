import os
import csvformat


TEST_PATH = os.path.dirname(__file__)

def test_csv_import():
    CSV_FILES_PATH = os.path.join(TEST_PATH, "tests/data/bluebike_test_data")
    files = csvformat.get_csv_files(CSV_FILES_PATH)

    assert len(files) == 2

