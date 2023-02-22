## Part 3: Write a pipeline with the Astro Cloud IDE

Parts 1 and 2 showed how easy it can be to get started using Airflow locally and write simple pipelines. Now in Part 3, we'll tackle a slightly more complex use case of combining two more complicated datasets. We will use the Astro Cloud IDE to develop a pipeline that compares the number of endangered species present in a country with the amount of protected area that country has set aside for wildlife with the goal of making a recommendation for countries to prioritize for conservation efforts.

The data is publicly available and can be found [here](https://www.kaggle.com/datasets/sarthakvajpayee/global-species-extinction).

### Step 1: Review the data

Go to `include/data/` and look at the two datasets we will use for this part: `country_terrestrial_protected_area.csv` and `country_species_status.csv`. The first dataset, `country_terrestrial_protected_area.csv`, is a time series of the percentage of land protected for each country from 1950-2021. You can see right away that both of these datasets are larger and more complex than the forest fires data of Part 2.

### Step 2: Create a pipeline with the Astro Cloud IDE

Go to cloud.astronomer.io and login with one of the login/password combinations provided. Click on the `Astro SF Workshop` workspace, and then `Cloud IDE` in the left menu. You should see one project called `Astro SF Workshop` - click in this project, and create a new pipeline using the `+ Pipeline` button. Give your pipeline a unique name to identify it.

### Step 3: Add global imports

In the first global imports cell, add the following:

`import pandas as pd`

### Step 4: Import and review species status data

Create a new SQL cell called `species_by_country`. In the connection drop down, choose the `snowflake_workshop` connection that is pre-populated.

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
FROM "CLIMATE"."CLIMATE"."COUNTRY_SPECIES_STATUS";
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

### Step 6: Import and review protected area data

Create a new SQL cell called `terrestrial_protected_area`. In the connection drop down, choose the `snowflake_workshop` connection that is pre-populated.

Enter the following SQL code in the cell, and then run it.


```sql
SELECT
"cou",
"Country",
"Year",
"Unit",
"Value"
FROM "CLIMATE"."CLIMATE"."COUNTRY_TERRESTRIAL_PROTECTED_AREA";
```

### Step 7: Transform the protected area data

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

### Step 8: Combine the two datasets

Create a new Python cell called `combine_data`. Enter the following Python code into the cell:

```python
return transform_endangered_species.merge(
    transform_tpa,
    how = 'inner',
    on = ['cou', 'Country']
).fillna(value = 'NA')
```

This creates a new temporary dataset with the joined data.

### Step 9: Analyze the results by ranking the 10 countries with the most critically endangered species

Create a new SQL cell called `rank_and_save`. In the connection drop down, choose the `snowflake_workshop` connection that is pre-populated.

```sql
select * from {{combine_data}}
order by "endangered_species" desc
limit 10;
```

Run the cell and review the results to determine which countries are conservation priorities.
