## Part 2: ELT pipeline

Part 1 showed how easy it can be to get started using Airflow with the Cloud IDE. Now in Part 2, we'll tackle a slightly more complex use case of combining two complicated datasets. We will use the Cloud IDE to develop a pipeline that compares the number of endangered species present in a country with the amount of protected area that country has set aside for wildlife with the goal of making a recommendation for countries to prioritize for conservation efforts. Since we are unfamiliar with this data to start, we'll use the Cloud IDE to iteratively develop our pipeline.

The data is publicly available and can be found [here](https://www.kaggle.com/datasets/sarthakvajpayee/global-species-extinction).

### Step 1: Create a pipeline with the Astro Cloud IDE

Create a new pipeline in your workspace. Give your pipeline a unique name to identify it.

### Step 2: Add global imports

In the first global imports cell, add the following:

`import pandas as pd`


### Step 3: Load Species Status Data

Create a new Python cell called "load_species_status_data" and add the following code to the cell:

```python
return pd.read_csv("https://raw.githubusercontent.com/astronomer/cli-cloud-ide-workshop/main/include/data/country_species_status_cleaned.csv", on_bad_lines='skip', nrows=100)
```

### Step 4: Import and review species status data

Create a new SQL cell called `species_by_country`. In the connection drop down menu, choose the In-memory SQL database.

Enter the following SQL code in the cell, and then run it.

```sql
SELECT 
    "iucn",
    "IUCN Category",
    "spec",
    "Species",
    "cou",
    "Country",
    "Value"
FROM {{ load_species_status_data }};
```

### Step 5: Transform the endangered species data

Create a new Python cell called `transform_endangered_species`. Enter the following Python code into the cell:

```python
all_species = species_by_country

endangered_species = all_species[(all_species['Species']!='Fish') & (all_species['IUCN Category'] == 'Number of critically endangered species')]
endangered_species_agg = endangered_species[['cou', 'Country', 'Value']].groupby(['cou', 'Country']).sum().reset_index()

endangered_species_agg.rename(
   columns = { 'Value': 'endangered_species' },
   inplace = True  
)

return endangered_species_agg
```

This takes the data returned in the previous cell and completes a couple of transformations:

1. Filters out fish species (because we're only reviewing terrestrial data here - we still care about fish!)
2. Filters to only the Critically Endangered Species IUCN category
3. Calculates the total number of critically endangered species by country using `groupby`.


### Step 6: Load Terrestrial Protected Area Data

Create a new Python cell called "load_terrestrial_protected_area_data" and add the following code to the cell:

```python
return pd.read_csv("https://raw.githubusercontent.com/astronomer/cli-cloud-ide-workshop/main/include/data/country_terrestrial_protected_area_cleaned.csv", on_bad_lines='skip', nrows=100)
```

### Step 7: Import and review protected area data

Create a new SQL cell called `terrestrial_protected_area`. In the connection drop down, choose the In-memory SQL database.

Enter the following SQL code in the cell, and then run it.


```sql
SELECT
"cou",
"Country",
"Year",
"Unit",
"Value"
FROM {{ load_terrestrial_protected_area_data }};
```

### Step 8: Transform the protected area data

Create a new Python cell called `transform_tpa`. Enter the following Python code into the cell:

```python
tpa = terrestrial_protected_area

tpa_2021 = tpa[tpa['Year']==2021][['cou', 'Country', 'Year', 'Value']]
tpa_2021['Value'] = tpa_2021['Value'].round(1)

tpa_2021.rename(
   columns = { 'Value': 'protected_area' },
   inplace = True  
)

return tpa_2021
```

This takes the data returned in the previous cell and completes a couple of transformations:

1. Filters to the year 2021 so we are only looking at the most recent data.
2. Rounds the percentage of protected area so the numbers are easier to review.
3. Renames the `Value` column so that we can clearly join the two datasets later.

### Step 9: Combine the two datasets

Create a new Python cell called `combine_data`. Enter the following Python code into the cell:

```python
return transform_endangered_species.merge(
    transform_tpa,
    how = 'inner',
    on = ['cou', 'Country']
).fillna(value = 'NA')
```

This creates a new temporary dataset with the joined data.

### Step 10: Analyze the results by ranking the 10 countries with the most critically endangered species

Create a new SQL cell called `rank_and_save`. In the connection drop down menu, choose the In-memory SQL database.

```sql
select * from {{combine_data}}
order by "endangered_species" desc
limit 10;
```

Run the cell and review the results to determine which countries are conservation priorities.
