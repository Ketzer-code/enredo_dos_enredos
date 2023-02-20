# importing libraries
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup

# requesting the discography page
discography_page = "https://pt.wikipedia.org/wiki/Discografia_do_Grupo_Especial_do_Rio_de_Janeiro"
page_req = requests.get(discography_page)

# creating soup to associate with headers
soup = BeautifulSoup(page_req.text, "lxml")

# getting all the discography tables
discography_tables = soup.find_all("table", {"class":"wikitable"})

# converting all tables to strings to read tables with pandas
discography_tables = (str(table) for table in discography_tables)

# reading in such a way that tables that containg multindexes are not multinidexed
discography_tables = (
    pd.read_html(table, header = 1) if any(["Lado A" in table, "Disco 1" in table, "Oficial" in table]) 
    else pd.read_html(table) for table in discography_tables
)

# some tables are returned as lists - selecting the first element of each list
discography_tables = (
    table[0] if isinstance(table, list) else table for table in discography_tables
)

# selecting only columns that we will use
discography_tables = [
    table.filter(items = [
        "Escola", "Enredo", "Autor", "Intérprete", "Intérprete(Participação Especial)", "Intérprete (Participação Especial)"
        ]) for table in discography_tables
]

# padronizing names
for table in discography_tables:
    if not table.empty:
        table.columns = ["escola", "enredo", "autor", "interpete"]


#collecting years
discography_years = soup.select("h2 span.mw-headline")

# treating list of years to contain only the year
discography_years = (str(y) for y in discography_years)
discography_years = (y.split("\"")[3] for y in discography_years)
discography_years = [y for y in discography_years if "19" in y or "20" in y]

# relating years with respective discography
# some years have more than one disk - 1968, 1970, 1971, 1985 and 1988 - multiplying
# those to match table list len
years_to_duplicate = [1968, 1970, 1971, 1985, 1988]
indexes_to_duplicate = ((y - 1968) + 1 for y in years_to_duplicate)
years_to_duplicate = (str(y) for y in years_to_duplicate)

index_year = dict(zip(indexes_to_duplicate, years_to_duplicate))
# creating counter to account for added indexes
aux = 0
for k, v in index_year.items():
    discography_years.insert(k + aux, v + "_b")
    aux = aux + 1

# to add each year as a column for each dataframe, we need to transform
# the list in a list of lists and multiply them for the lenght of the corresponding dataframe
year_table = dict(zip(discography_years, discography_tables))
discography_years = ([y.replace("_b", "")] * len(year_table.get(y)) for y in year_table)

# picking each list and inserting into each dataframe as a new column
for table, year in zip(discography_tables, discography_years):
    table.loc[:, "ano"] = year

# joining dataframe list into a single dataframe
discography_df = pd.concat(discography_tables)

# dropping rows that are actually columns from multindex tables BATERIA DAS ESCOLAS Baterias das escolas
undesirable_rows = [
    "Lado B", "Intérprete", "Disco 2", 
    "BATERIA DAS ESCOLAS", "Baterias das escolas"
]

mask_drop_rows = discography_df.apply(
    lambda row: any(
        [re.findall(r"|".join(undesirable_rows), string) for string in row if isinstance(string, str)]
        ), axis = 1
)

discography_df_filtered = discography_df[~mask_drop_rows]

discography_df_filtered.to_csv("discografia_enredos.csv", index = False)