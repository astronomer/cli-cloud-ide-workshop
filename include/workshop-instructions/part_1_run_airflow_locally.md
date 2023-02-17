## Part 1: Run Airflow Locally

The first part of this workshop will cover getting started with Airflow locally and running a simple DAG that uses dynamic task mapping.

### Step 1: Start Airflow

Using the [Astro CLI](https://docs.astronomer.io/astro/cli/overview), start the project by running the following from this project directory in your terminal:

```bash
astro dev start
```

### Step 2: Explore the Airflow UI

Go to localhost:8080 in your browser and explore the Airflow UI. You should see three DAGs in your Airflow environment.

### Step 3: Run a simple example DAG

Unpause the `example_dag_basic` DAG and trigger it. Go to the Grid View and check how many mapped instances were created for the `consumer` task. 

Try running the DAG again and see if the number of mapped instances changed.
