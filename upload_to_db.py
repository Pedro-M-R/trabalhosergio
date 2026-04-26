import csv
import glob
import io
import os
import re

import pandas as pd
import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "host": "dataiesb.iesbtech.com.br",
    "dbname": "2312120008_Pedro",
    "user": "2312120008_Pedro",
    "password": "2312120008_Pedro",
    "connect_timeout": 10,
}
CSV_DIR = "baixados_sia"
CSV_PATTERN = "SIA_*.csv"


def clean_cell(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    text = re.sub(r'^"+|"+$', '', text).strip()
    return None if text == "-" else text


def sanitize_column_name(name):
    if name is None:
        name = "column"
    name = str(name).strip()
    name = re.sub(r'^"+|"+$', '', name).strip()
    name = name.lower()
    name = re.sub(r"[\n\r]+", " ", name)
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "column"
    if re.match(r"^[0-9]", name):
        name = f"c_{name}"
    return name


def resolve_column_names(columns):
    sanitized = [sanitize_column_name(col) for col in columns]
    counts = {}
    result = []
    for col in sanitized:
        existing = counts.get(col, 0)
        if existing:
            new_name = f"{col}_{existing}"
        else:
            new_name = col
        counts[col] = existing + 1
        result.append(new_name)
    return result


def load_csv(path):
    print(f"Loading CSV: {path}")
    if path.lower().endswith("_certo.csv"):
        df = pd.read_csv(
            path,
            sep=",",
            header=0,
            index_col=0,
            encoding="utf-8-sig",
            dtype=str,
            low_memory=False,
            na_filter=False,
        )
    else:
        df = pd.read_csv(
            path,
            sep=";",
            header=1,
            encoding="utf-8-sig",
            dtype=str,
            low_memory=False,
            na_filter=False,
        )
    df = df.apply(lambda col: col.map(clean_cell))
    df.columns = resolve_column_names(df.columns)
    return df


def create_table(cursor, table_name, columns):
    identifiers = [sql.Identifier(column) for column in columns]
    create_sql = sql.SQL(
        "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({cols})"
    ).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(
            sql.SQL("{} text").format(identifier) for identifier in identifiers
        ),
    )
    cursor.execute(create_sql)


def upload_dataframe(connection, df, table_name):
    with connection.cursor() as cursor:
        print(f"Creating table: {table_name} ({len(df.columns)} columns)")
        create_table(cursor, table_name, df.columns)

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, sep=";", header=True, quoting=csv.QUOTE_MINIMAL)
        buffer.seek(0)

        copy_sql = sql.SQL(
            """COPY {table} ({cols}) FROM STDIN WITH CSV HEADER DELIMITER ';' QUOTE '"'"""
        ).format(
            table=sql.Identifier(table_name),
            cols=sql.SQL(", ").join(sql.Identifier(column) for column in df.columns),
        )
        print(f"Uploading {len(df)} rows into {table_name}")
        cursor.copy_expert(copy_sql, buffer)
    connection.commit()
    print(f"Finished upload: {table_name}")


def main():
    paths = sorted(glob.glob(os.path.join(CSV_DIR, CSV_PATTERN)))
    paths = [path for path in paths if '_certo' in os.path.basename(path).lower()]
    if not paths:
        raise SystemExit(f"No CSV files found in {CSV_DIR} matching {CSV_PATTERN} containing _certo")

    with psycopg2.connect(**DB_CONFIG) as conn:
        for path in paths:
            table_name = os.path.splitext(os.path.basename(path))[0].lower()
            table_name = re.sub(r"_certo$", "", table_name)
            table_name = re.sub(r"[^a-z0-9_]+", "_", table_name).strip("_")
            df = load_csv(path)
            upload_dataframe(conn, df, table_name)


if __name__ == "__main__":
    main()
