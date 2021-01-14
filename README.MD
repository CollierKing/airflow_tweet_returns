# Experimenting with Apache Airflow 
This repo holds a project to demonstrate my first foray into [Apache Airflow]().  Airflow is perfect for architecting sophisticated and robust ETL processes using Python.  So here goes!

## Use Case 
My use case is as follows.  

1. A client wants to showcase their Stock Picking skills to their Twitter followers.  
2. They have been tweeting out stock picks and they want a daily report showing the Stock returns since their first tweet mentioning said Stock.
3. Oh yeah they want it loaded to a Google Sheet as soon as possible after the market closes.

## Process Diagram
So we have a multi-step data-pipeline with a business logic & computations layer.  

The key requirement is the speed of delivering the daily report.  To meet this we will look to execute the business logic & computations layer across parallel tasks.

Our entire pipeline process looks something like this:

## Airflow Diagram
The diagram of our DAG in Airflow looks like this:

## Airflow Project Structure
The tree of our Airflow project looks like this:

Our tweet_processing_dag.py file contains our DAG.

Some custom Hooks and Operators are in our Plugins folder.  We have 4 of them needed to do the job:
1. Google Sheets Plugin - handles pushing the final dataset to the client
2. Pandas_DB Plugin - handles pushing and pulling Postgres data to and from Pandas
3. ThinkOrSwim (ToS) Plugin - handles pulling historical stock quotes for the tickers our client tweets
4. Twitter Plugin - handles pulling our client's tweets from Twitter

## Conclusion
With the help of Airflow's parallel processing, logging, dependency management, builtin connection and plugin extendability capabilities, we were able to create a pretty extensive ETL pipeline for our client.
Airflow's UI and modular design make it easy to test, debug and enhance as well.

This is just the beginning of my Airflow journey, hoping to have more pipelines up and running soon!

Checkout the final Google Sheet of Stock Tickers, Returns and Mention Dates here:
https://docs.google.com/spreadsheets/u/2/d/e/2PACX-1vSYgMf87bi4i07jgcTAjN630agkAAs_G6Ai2kUQ4k-ar0Qv3dEhfviGF52IYhk3O2j8ZyqXEUz4qAxK/pubhtml
