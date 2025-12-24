from pathlib import Path
import pandas as pd

from tableauhyperapi import (
    HyperProcess, Connection, TableDefinition, SqlType,
    Inserter, Telemetry, CreateMode
)


def export_hyper(df: pd.DataFrame, path: Path, hyper_table: str):
    table = TableDefinition(
        hyper_table,
        [
            TableDefinition.Column("BANK", SqlType.text()),
            TableDefinition.Column("TEXT", SqlType.text()),
            TableDefinition.Column("TYPE", SqlType.text()),
            TableDefinition.Column("DATE", SqlType.text()),
        ],
    )

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
        with Connection(hp.endpoint, str(path), CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_table(table)
            rows = df[["BANK", "TEXT", "TYPE", "DATE"]].where(pd.notnull(df), None).values.tolist()
            with Inserter(conn, table) as ins:
                ins.add_rows(rows)
                ins.execute()
