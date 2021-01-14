from typing import Dict, Any, List, Tuple
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults
from pandas import DataFrame


class PandasPostgresOperator(PostgresOperator):

    @apply_defaults
    def __init__(self, destination_table: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.destination_table: str = destination_table

    def execute(self, action="insert", df: DataFrame = False, sql=False):
        # create PostgresHook
        self.hook: PostgresHook = \
            PostgresHook(
                postgres_conn_id=self.postgres_conn_id,
                schema=self.database,
            )
        # read data from Postgres-SQL query into pandas DataFrame
        # df: DataFrame = self.hook.get_pandas_df(sql=self.sql, parameters=self.parameters)
        # perform transformations on df here
        # df['column_to_be_doubled'] = df['column_to_be_doubled'].multiply(2)
        if action == "insert":
            # convert pandas DataFrame into list of tuples
            rows: List[Tuple[Any, ...]] = list(df.itertuples(index=False, name=None))
            # insert list of tuples in destination Postgres table
            self.hook.insert_rows(table=self.destination_table, rows=rows)
        else:
            hook_conn_obj = self.hook.get_conn()
            hook_conn_obj.autocommit = True
            df: DataFrame = pd.io.sql.read_sql(sql=sql, con=hook_conn_obj)
            # df: DataFrame = self.hook.get_pandas_df(sql=sql, parameters=self.parameters)
            return df
