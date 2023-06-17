import os
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
db_url = os.getenv('DB_URL')

def get_dataframe(url):
    return pd.read_csv(url,index_col=0)

# Instead of downloading the csv files,read data directly from the mocked urls
customer_data = get_dataframe("https://raw.githubusercontent.com/roshna-k-abdulsalam/travel-booking-dataset/main/customer.csv")
destination_data = get_dataframe("https://github.com/roshna-k-abdulsalam/travel-booking-dataset/raw/main/destination.csv")
booking_data = get_dataframe("https://raw.githubusercontent.com/roshna-k-abdulsalam/travel-booking-dataset/main/booking.csv")

engine = create_engine(db_url)
conn = engine.connect()

#Cleanse customer data
customer_data = customer_data.dropna()

#Cleanse destination data
destination_data['popular_season'] = destination_data['popular_season'].str.capitalize()
destination_data = destination_data.dropna()

#Load customers to Customer table 
customer_data.to_sql('customer', conn, if_exists='append', index=True)
print("Customer loaded successfully")
time.sleep(10)

#Load destinations to Destination table
destination_data.to_sql('destination', conn, if_exists='append', index=True)
print("Destination loaded successfully")
time.sleep(10)

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
print("Booking loaded successfully")

conn.close()
