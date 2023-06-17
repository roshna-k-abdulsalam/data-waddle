# Script to read data from the downloaded dataset, cleanse, transform and load them to database

import os
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
db_url = os.getenv('DB_URL')

def get_dataframe(url):
    return pd.read_csv(url,index_col=0)

current_path =  os.getcwd()
download_dir = os.path.join(os.getcwd(), "download_dataset")

#Check whether download directory exists 
if not os.path.exists(download_dir):
    raise FileNotFoundError(f"The directory path {download_dir} does not exist.")

#Check whether required files exists in the download directory 
download_dir_elements = os.listdir(download_dir)
for i in ['customer.csv', 'booking.csv', 'destination.csv']:
    if i not in download_dir_elements:
        raise FileNotFoundError(f"The file {i} does not exist in the path {download_dir}.")

customer_data = get_dataframe(f"{download_dir}/customer.csv")
destination_data = get_dataframe(f"{download_dir}/destination.csv")
booking_data = get_dataframe(f"{download_dir}/booking.csv")

engine = create_engine(db_url)  
conn = engine.connect()

#Cleanse customer data
customer_data = customer_data.dropna()

#Cleanse destination data
destination_data['popular_season'] = destination_data['popular_season'].str.capitalize()
destination_data = destination_data.dropna()

#Load customers to Customer table 
customer_data.to_sql('customer', conn, if_exists='append', index=True)
print("Customer data loaded successfully")
time.sleep(5)

#Load destinations to Destination table
destination_data.to_sql('destination', conn, if_exists='append', index=True)
print("Destination data loaded successfully")
time.sleep(5)

#Cleanse booking data
booking_data['booking_date'] = pd.to_datetime(booking_data['booking_date'])
destination_mapping = pd.read_sql_query('SELECT destination_id, destination FROM destination', conn)
booking_data = booking_data.merge(destination_mapping, on='destination', how='left')    
booking_data = booking_data.drop(columns=['destination'])

booking_data.fillna({"cost_per_passenger": booking_data['cost_per_passenger'].median(), 
                    "number_of_passengers": 1 }, 
                    inplace=True)

booking_data = booking_data.dropna(subset=['destination_id', 'customer_id', 'booking_id', 'booking_date']) 

# Calculate total booking value
booking_data['total_booking_value'] = booking_data['number_of_passengers'] * booking_data['cost_per_passenger']

#Load booking data to Booking table
booking_data.to_sql('booking', conn, if_exists='append', index=False)
print("Booking data loaded successfully")

conn.close()