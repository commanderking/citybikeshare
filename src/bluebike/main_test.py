import os
import csvformat

TEST_PATH = os.path.dirname(__file__)
EXPECTED_COLUMNS = csvformat.renamed_columns.values()

def test_csv_import():
    CSV_FILES_PATH = os.path.join(TEST_PATH, "tests/testdata")
    files = csvformat.get_csv_files(CSV_FILES_PATH)

    assert len(files) == 2

def test_formats_df_correctly():
    CSV_FILES_PATH = os.path.join(TEST_PATH, "tests/testdata")
    files = csvformat.get_csv_files(CSV_FILES_PATH)
    df = csvformat.create_formatted_df(files)

    assert len(df.index) == 10
    assert all([col in df.columns for col in EXPECTED_COLUMNS])

    birth_year_check = df.loc[df["birth_year"] == 1994]
    assert len(birth_year_check) == 1

    gender_check = df.loc[df["gender"] == 0]
    assert len(gender_check) == 1

