import os
import boston

TEST_PATH = os.path.dirname(__file__)

pre_march_2023_columns = boston.renamed_columns_pre_march_2023.values()

march_2023_and_beyond_columns = boston.renamed_columns_march_2023_and_beyond.values()

all_headers = list(set(pre_march_2023_columns) | set(march_2023_and_beyond_columns))

def test_csv_import():
    CSV_FILES_PATH = os.path.join(TEST_PATH, "tests/testdata")
    files = boston.get_csv_files(CSV_FILES_PATH)

    assert len(files) == 3

def test_formats_df_correctly():
    CSV_FILES_PATH = os.path.join(TEST_PATH, "tests/testdata")
    files = boston.get_csv_files(CSV_FILES_PATH)
    df = boston.create_formatted_df(files)

    assert len(list(df)) == len(all_headers)

    assert all([col in df.columns for col in all_headers])

    birth_year_check = df.loc[df["birth_year"] == 1994]
    assert len(birth_year_check) == 1

    gender_check = df.loc[df["gender"] == 0]
    assert len(gender_check) == 1

