import polars as pl


def print_null_data_df(df):
    df_null_rows = df.filter(pl.any_horizontal(pl.all().is_null()))
    print(df_null_rows)
    return df
