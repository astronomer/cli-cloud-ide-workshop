## Part 2: Write an ELT DAG with the Astro SDK

The second part of this workshop will cover developing a simple ELT DAG using the Astro Python SDK.

### Step 1: Review the forest fire data

This DAG processes forest fire data that can be found in `include/data/forestfires.csv`. The data is straightforward and small in size - take a look at it in your code editor or in the GitHub repo from your browser.

This data is publicly available and can be found [here](https://archive.ics.uci.edu/ml/datasets/forest+fires).

### Step 2: Review the forest fire ETL DAG code

In the Airflow UI Code view or in your code editor, review the `forest_fire_etl` DAG. This DAG uses the [Astro Python SDK](https://github.com/astronomer/astro-sdk) to run a basic ETL pipeline on the forest fire data.

The DAG completes the following steps:

1. Loads the raw file from local storage into a DuckDB instance running in this Airflow environment.

2. Filters the data to only fires that had a burned area greater than 0 acres.

3. Transforms the data to calculate the average area burned per month and saves those results to a new table.

### Step 3: Run the DAG

Trigger the `forest_fire_etl` DAG to run the ETL pipeline. 

### Step 4: Review results

The final reporting table with average area burned per month is saved in your local DuckDB instance. The easiest way to view the results is to run the `show_etl_results` DAG and look at the logs of the `get_result` task.
