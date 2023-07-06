## Part 1: Airflow basics

In Part 1 of this workshop you will create a simple pipeline gathering information about [manatees](https://www.savethemanatee.org/manatees/facts/). You'll get to practice with basic Airflow features like operators, connections, and variables.

### Step 1: Create a pipeline with the Astro Cloud IDE

Go to cloud.astronomer.io and login to your trial account. In your workspace, click `Cloud IDE` in the left menu. Create a new project, and then create a new pipeline using the `+ Pipeline` button. Give your pipeline a unique name to identify it.

### Step 2: Add Global Imports

In the first global imports cell, add the following:

```python
import random

def num_manatees():
    aggregation_size = random.randint(1, 6)
    return f"This aggregation has {aggregation_size} manatees!"
```

This cell will import any Python packages needed for your pipeline, as well as any Python functions you'll be calling.

### Step 3: Add a Python task

1. Create a new PythonOperator cell called `count_the_manatees`.

2. Add `num_manatees` to the `python callable` parameter. This task calls the function in the Global Imports cell, which determines the size of the aggregation of manatees (a random number between 1 and 6).

3. Run the task to see the number of manatees.

### Step 4: Add a variable

It looks like a new baby manatee has arrived! In your next task you'll name the new manatee.

Go to the Environment tab, select Variables, and add a new Variable called `MANATEE_NAME`. Fill in the value with your chosen name for the baby manatee.

You can learn more about Airflow variables [here](https://docs.astronomer.io/learn/airflow-variables).

### Step 5: Add a Bash task

Now you'll create a task using a different Airflow operator that will access the variable you just created.

1. Create a new BashOperator cell called `name_the_baby_manatee`. The BashOperator runs any Bash command, and can often be useful for running tasks in other languages.

2. For this task, enter the following for the `Bash Command` parameter:

    ```bash
    echo "The baby manatee's name is {{ var.value.MANATEE_NAME }}."
    ```

This command accesses the manatee name variables with `{{ var.value.MANATEE_NAME }}`. This expression will only be evaluated at run time, and is a good way of providing information that doesn't change frequently to your tasks.

3. Run the cell and review the results.

4. Set the cell to have the `count_the_manatees` task as an upstream dependency.

### Step 6: Add a connection

One of Airflow's strengths is its ability to connect to nearly any external system. This is usually accomplished using an [Airflow connection](https://docs.astronomer.io/learn/connections).

For your next task, you'll connect to an API that returns jokes about manatees.

1. Go to the Environment tab, choose Connections, and add a connection. Choose the Generic connection type.

2. Enter the following information into your connection:

    - **Connection ID**: http_default
    - **Connection Type**: HTTP
    - **Host**: https://manateejokesapi.herokuapp.com/

    This API does not require an API key, so no additional information is needed for the connection.

### Step 7: Add an HTTP task

1. Create a new SimpleHttpOperator cell called `get_a_manatee_joke`.

2. Fill out the following parameters:

    - Endpoint: manatees/random/
    - Method: GET
    - HTTP Conn ID: http_default

3. Run the task to see your manatee joke.

4. Set the cell to have `name_the_baby_manatee` as an upstream dependency.

### Step 8: Run and review the full pipeline

1. Review the graph of your pipeline now that all tasks and dependencies have been defined.

2. Run the full pipeline using the **Run** button in the upper right corner.

3. Look at the Code tab to see the DAG code that was created automatically for you.
