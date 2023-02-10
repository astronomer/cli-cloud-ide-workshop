from datetime import datetime
from pandas import DataFrame

from airflow.decorators import dag

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.sql.table import Table

connection_id='duckdb_default'

@aql.transform
def filter_fires(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE area > 0"


@aql.dataframe
def avg_fire_area(df: DataFrame):
    df['year'] = 2023
    forest_fire_monthly_avg = df[['year', 'month', 'area']].groupby(['year', 'month']).median().reset_index()
    return forest_fire_monthly_avg


@dag(schedule=None, start_date=datetime(2023, 2, 1), catchup=False)
def forest_fire_etl():

    load_ff_data = aql.load_file(
        input_file=File(path='/usr/local/airflow/include/data/forestfires.csv', filetype=FileType.CSV),
        # input_file=File(path='s3://airflow-kenten/forestfires.csv', filetype=FileType.CSV, conn_id='AWS'),
        output_table=Table(name="raw_forestfires", conn_id=connection_id),
        if_exists='replace',
        use_native_support=False
    )

    monthly_avg_burned = avg_fire_area(filter_fires(load_ff_data), output_table=Table(name="monthly_avg_area_burned"))

    aql.cleanup()

dag = forest_fire_etl()
