## Part 3: Write a pipeline with the Astro Cloud IDE


Add the following to the first global imports cell

`import pandas as pd`

species_by_country - 
```sql
SELECT 
    IUCN,
    "IUCN Category",
    SPEC,
    "Species",
     COU,
    "Country",
    "Value"
FROM "SANDBOX"."KENTENDANAS"."COUNTRY_VULNERABLE";
```

transform_endangered_species - 
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

terrestrial_protected_area - 
```sql
SELECT
 COU,
"Country",
"Year",
"Unit",
"Value"
FROM "SANDBOX"."KENTENDANAS"."COUNTRY_TERRESTRIAL_PROTECTED_AREA";
```

transform_tpa -
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

combine_data - 
```python
return transform_endangered_species.merge(
    transform_tpa,
    how = 'inner',
    on = ['cou', 'Country']
).fillna(value = 'NA')
```

rank_and_save - 
```sql
select * from {{combine_data}}
order by "endangered_species" desc
limit 10;
```