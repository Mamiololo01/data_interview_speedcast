#import libraries
import pandas as pd
import os
from etl import extract_data 
from util import get_database_conn, create_tables, insert_into_dim_city, insert_into_dim_date, insert_into_fact_cloud_cover, get_city_id, get_date_id