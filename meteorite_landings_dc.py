# pip install python-dotenv         # NOT IN CONDA
# pip install sodapy                # NOT IN CONDA
# pip install reverse_geocoder      # NOT IN CONDA

# Import needed libraries.  Unless noted above, all libraries are available in the baseline conda environment.
import pandas as pd
from dotenv import load_dotenv
from sodapy import Socrata
import reverse_geocoder as rg
import csv
import os

# Load .env file and obtain environment variables
load_dotenv()
APP_TOKEN = os.getenv("APP_TOKEN")

def get_meteorite_data():
    # Establish Socrata client with APP_TOKEN
    client = Socrata("data.nasa.gov", APP_TOKEN)

    # Page through results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.  This should get all entries (45,716 as of 2021-11-14)
    query_offset = 0
    query_limit = 2000
    query_active = True
    df_pages = []

    while query_active:          
        results = client.get("gh4g-9sfh", limit=query_limit, offset=query_offset)
        results_df = pd.DataFrame.from_records(results)
        df_pages.append(results_df)
        if len(results_df) < query_limit:
            query_active = False
        else:
            query_offset += query_limit

    # Convert to pandas DataFrame
    meteorite_data = pd.concat(df_pages, ignore_index=True)

    print("Queried NASA for all meteorite landings.")
    print("Found and collected", meteorite_data.shape[0], "entries.")
    return meteorite_data

def process_locations(meteorite_data):
    # Drop extra geolocation columns that we don't need
    meteorite_data.drop(['geolocation', ':@computed_region_cbhk_fwbd', ':@computed_region_nnqa_25f4'], axis=1, inplace=True)

    # Convert lat/long to floats
    meteorite_data['reclong'] = meteorite_data['reclong'].astype(float)
    meteorite_data['reclat'] = meteorite_data['reclat'].astype(float)
    
    # Drop data with missing lat/long values.
    orig_rows = meteorite_data.shape[0]
    meteorite_data.dropna(subset=['reclong', 'reclat'], inplace=True)
    na_rows = orig_rows - meteorite_data.shape[0]

    # Validate lat/long values.  Lat values should be -90 to 90; Long values should be -180 to 180.
    invalid_coords = (meteorite_data['reclong'] < -180) | (meteorite_data['reclong'] > 180) | (meteorite_data['reclat'] < -90) | (meteorite_data['reclat'] > 90)
    invalid_rows = meteorite_data[invalid_coords].shape[0]

    rows_to_drop = meteorite_data[invalid_coords].index
    meteorite_data.drop(rows_to_drop, inplace=True)

    new_rows = meteorite_data.shape[0]

    print('Dropped', na_rows, 'missing locations.')
    print('Dropped', invalid_rows, 'invalid locations.')
    print('Total valid entries: ', new_rows)

    return meteorite_data

def get_country_data(meteorite_data):

    country_names = pd.read_csv('countries_codes_and_coordinates.csv', squeeze=True, keep_default_na=False, quoting=csv.QUOTE_ALL)
    country_names['Alpha-2 code'] = country_names['Alpha-2 code'].str.replace('"', '').str.strip()
    country_names['Alpha-3 code'] = country_names['Alpha-3 code'].str.replace('"', '').str.strip()
    country_names['Numeric code'] = country_names['Numeric code'].str.replace('"', '').str.strip()
    country_names.set_index('Alpha-2 code', inplace=True)

    map_data = pd.DataFrame(columns = ['country_name', 'country_code_2', 'country_code_3', 'admin1', 'admin2'], index = meteorite_data.index)

    coordinates = tuple(zip(meteorite_data['reclat'], meteorite_data['reclong']))
    rg_result = rg.search(coordinates)

    for idx, result in enumerate(rg_result):
        result_parsed = {}
        result_list = []

        result_parsed['country_code_2'] = result['cc']
        result_parsed['admin1'] = result['admin1']
        result_parsed['admin2'] = result['admin2']

        lookup_cc = result_parsed['country_code_2']
        result_parsed['country_name'] = country_names.loc[lookup_cc]['Country']
        result_parsed['country_code_3'] = country_names.loc[lookup_cc]['Alpha-3 code']

        # Order correctly for row update.
        result_list.append(result_parsed['country_name'])
        result_list.append(result_parsed['country_code_2'])
        result_list.append(result_parsed['country_code_3'])
        result_list.append(result_parsed['admin1'])
        result_list.append(result_parsed['admin2'])

        map_data.iloc[idx] = result_list

    print('Added country data to the dataset.') 
    return meteorite_data.join(map_data)
