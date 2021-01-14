import time

import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

import ast
from pandas_db_plugin.operators.pandas_to_postgres_operator import PandasPostgresOperator
from tos_plugin.hooks.tos_hook import ToSHook
from twitter_plugin.hooks.twitter_hook import TwitterHook
from google_sheets_plugin.hooks.google_sheets_hook import GSheetsHook

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2020, 1, 1)
}


# helper functions
def execute_pd_sql(df, destination_table=False, sql=False):
    # insert tweets
    pdpg = PandasPostgresOperator(
        task_id='execute_sql_task1',  # task ids need to vary across same operators
        sql=sql,
        destination_table=destination_table)
    pdpg.execute(df=df, action="insert")
    print("loaded records...")


def load_tweets(screen_name):
    """
    Pulls tweets from a Twitter user and loads them into Postgres
    :return:
    """
    tweet_hook = TwitterHook()
    # grab tweets
    tweet_df = tweet_hook.get_usr_tweets(screen_name=screen_name)
    return tweet_df


def query_tweets(limit=10):
    """
    Pulls tweets from Postgres for processing
    :return:
    """
    pdpg = PandasPostgresOperator(
        task_id='pandas_postgres_task2',  # ^^ task ids need to vary across same operators
        sql="",
        destination_table="tweets",
        autocommit=True
    )

    # get tickers read to be processed
    df = pdpg.execute(
        action="pull",
        sql=f'''
        UPDATE tweets as t1
        SET 
        status = 'busy' /*mark them as busy so other tasks dont pick them up*/
        FROM (
            SELECT * 
            FROM tweets
            WHERE status = 'ready'
            ORDER BY ticker
            LIMIT {limit}
            FOR UPDATE SKIP LOCKED
        ) t2
        WHERE t1.ticker = t2.ticker
        RETURNING t2.*
        '''
    )

    return df


def get_quotes(ticker, start_date=False):
    """
    Pulls quotes for specified tickers
    :return:
    """
    tos_hook = ToSHook()
    quotes_df = tos_hook.pull_quotes(ticker, start_date)
    print(quotes_df.head())
    return quotes_df


def calculate_returns(df):
    """
    Calculate returns for specified tickers
    :return:
    """
    df_delta = \
        pd.DataFrame({  # high
            "high_delta": [(df.head(1)['high'].values[0] - \
                            df.tail(1)['high'].values[0]) / df.tail(1)['high'].values[0]],
            # low
            "low_delta": [(df.head(1)['low'].values[0] - \
                           df.tail(1)['low'].values[0]) / df.tail(1)['low'].values[0]],
            # close
            "close_delta": [(df.head(1)['close'].values[0] - \
                             df.tail(1)['close'].values[0]) / df.tail(1)['close'].values[0]],
            # open
            "open_delta": [(df.head(1)['open'].values[0] - \
                            df.tail(1)['open'].values[0]) / df.tail(1)['open'].values[0]]
        })

    return df_delta


def push_results_to_google():
    pdpg = PandasPostgresOperator(
        task_id='pandas_postgres_task3',  # ^^ task ids need to vary across same operators
        sql="",
        destination_table="results",
    )

    # get tickers read to be processed
    df = pdpg.execute(
        action="pull",
        sql=f'''select * from results'''
    )

    gs_hook = GSheetsHook()
    gs_hook.delete_data(
        sheet_id="12N3CZARjdRRhHdq5b21PrWMkB58xmAkCz2VgGjMbfqc",
        tab_id="0",
        idx_start=0,
        idx_end=999)

    gs_hook.push_data(
        sheet_id="12N3CZARjdRRhHdq5b21PrWMkB58xmAkCz2VgGjMbfqc",
        tab_name="Sheet1",
        df=df.astype(str)
    )


# wrapper functions
def wrp_tweet_etl(twtr_user_name="surinotes"):
    """
    1) to pull available tweets
     1) loads tweets into Postgres
    :return:
    """
    # pulls tweets from twitter user timeline
    tweet_df = load_tweets(screen_name=twtr_user_name)

    tweet_df = \
        tweet_df[
            tweet_df['symbols'] != ""
            ].reset_index(drop=True)

    # dedup
    tweet_df = \
        tweet_df[[
            "datetime", "symbols"
        ]].drop_duplicates().reset_index(drop=True)

    # strip ticker text
    tweet_df['symbols'] = \
        tweet_df['symbols'].astype(str).str.strip()

    # convert ticker list string to symbols
    tweet_df['symbols_list'] = \
        tweet_df.apply(lambda x: list(ast.literal_eval(repr(x['symbols'].split(",")))), axis=1)

    # explode out tickers
    tweet_df = \
        (tweet_df
         .explode("symbols_list")
         .drop("symbols", axis=1)
         )

    # strip again and convert to uppercase
    tweet_df['symbols_list'] = \
        tweet_df['symbols_list'].str.upper().str.strip()

    # drop dups and sort by symbol and datetime
    tweet_df = \
        (tweet_df
         .drop_duplicates()
         .sort_values(by=["symbols_list", "datetime"], ascending=[True, True])
         .reset_index(drop=True)
         )

    # assign index to tweet date (0...n)
    tweet_df['idx_cnt'] = \
        tweet_df.groupby('symbols_list').cumcount()

    # find first mention by ticker
    tweet_df_first = \
        (
            tweet_df
                .loc[tweet_df.groupby('symbols_list')['idx_cnt'].idxmin()]
                .reset_index(drop=True)[["datetime", "symbols_list"]]
                .rename(columns={"symbols_list": "ticker"})
                .assign(status="ready")
        )

    # loads tweets into postgres
    execute_pd_sql(df=tweet_df_first, destination_table="tweets")


def wrp_returns_calculation(delay, **kwargs):
    """
    Pipeline function:

     2) pull quotes for symbols in tweets
     3) calculate returns since tweet mention
     4) push to Postgres

    :return:
    """
    time.sleep(delay)

    tickers_left = 1
    iter_count = 0
    while tickers_left == 1:
        iter_count += 1
        print("-------------------------------------------------------")
        print(f"Ticker proc iter count: {iter_count}")
        tweet_df = query_tweets()

        df_concat_list = []
        if len(tweet_df) > 0:
            # for each tweet/symbol
            for idx, row in tweet_df.iterrows():

                print(row['ticker'])

                # pull stock quotes
                try:
                    quotes_df = get_quotes(
                        ticker=row['ticker'],
                        start_date=pd.to_datetime(row['datetime'])
                    )
                except Exception as e:
                    print(str(e))
                    continue

                # calculate the returns
                returns_df = calculate_returns(df=quotes_df)

                returns_df['ticker'] = row['ticker']
                returns_df['first_mention'] = row['datetime']

                # concatenate DFs
                df_concat_list.append(returns_df.copy())

                # sleep so we dont get throttled by ToS API
                time.sleep(1)

            returns_df_all = pd.concat(df_concat_list).reset_index(drop=True)

            # re-order columns
            returns_df_all = \
                returns_df_all[["ticker",
                                "first_mention",
                                "high_delta",
                                "low_delta",
                                "close_delta",
                                "open_delta"
                                ]].reset_index(drop=True)

            # load to postgres
            execute_pd_sql(df=returns_df_all, destination_table="results")
        else:
            tickers_left = 0


with DAG('parallel_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task')

    # drop tables
    drop_table_task1 = PostgresOperator(
        task_id='drop_table_task1',
        sql=f'''DROP TABLE tweets'''
    )

    drop_table_task2 = PostgresOperator(
        task_id='drop_table_task2',
        sql=f'''DROP TABLE results'''
    )

    # create tables
    create_table_task1 = PostgresOperator(
        task_id='create_table_task1',
        trigger_rule="all_done",
        sql= \
            f'''CREATE TABLE results (
                    ticker VARCHAR(5),
                    first_mention_date DATE,
                    high_delta FLOAT,
                    low_delta FLOAT,
                    close_delta FLOAT,
                    open_delta FLOAT
                    );'''
    )

    create_table_task2 = PostgresOperator(
        task_id='create_table_task2',
        trigger_rule="all_done",
        sql= \
            f'''CREATE TABLE tweets (
                datetime DATE,
                ticker VARCHAR(10),
                status VARCHAR(10)
                );'''
    )

    tweet_etl_task = PythonOperator(
        task_id='tweet_etl',
        python_callable=wrp_tweet_etl)

    returns_calculation_task1 = PythonOperator(
        task_id="returns_calculation_task1",
        python_callable=wrp_returns_calculation,
        op_kwargs={'delay': 0}
    )
    returns_calculation_task2 = PythonOperator(
        task_id="returns_calculation_task2",
        provide_context=True,
        python_callable=wrp_returns_calculation,
        op_kwargs={'delay': 1}
    )
    returns_calculation_task3 = PythonOperator(
        task_id="returns_calculation_task3",
        provide_context=True,
        python_callable=wrp_returns_calculation,
        op_kwargs={'delay': 2}
    )
    returns_calculation_task4 = PythonOperator(
        task_id="returns_calculation_task4",
        provide_context=True,
        python_callable=wrp_returns_calculation,
        op_kwargs={'delay': 3}
    )

    postgres_gsheets = PythonOperator(
        task_id="postgres_gsheets",
        trigger_rule="all_done",
        python_callable=push_results_to_google
    )

    start_task >> \
    drop_table_task1 >> \
    drop_table_task2 >> \
    create_table_task1 >> \
    create_table_task2 >> \
    tweet_etl_task >> \
    [
        returns_calculation_task1,
        returns_calculation_task2,
        returns_calculation_task3,
        returns_calculation_task4
    ] >> \
    postgres_gsheets
