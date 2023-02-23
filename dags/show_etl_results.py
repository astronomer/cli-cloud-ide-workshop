from datetime import datetime
import duckdb

from airflow.decorators import dag, task


@task
def get_results():
    cursor = duckdb.connect("etl_example")
    results = cursor.execute("SELECT * FROM monthly_avg_area_burned").fetchall()
    max_month = cursor.execute("SELECT month FROM monthly_avg_area_burned ORDER BY area DESC LIMIT 1;").fetchall()
    print(results)
    return results, max_month


@dag(schedule=None, start_date=datetime(2023, 2, 1), catchup=False)
def show_etl_results():

    show_results = get_results()

dag = show_etl_results()
