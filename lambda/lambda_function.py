# This is the lambda function file that reads destination and booking data from the s3 bucket 
# based on the current date there by calculating metrics like total_bookings_per_destination, 
# total_booking_value_per_destination, total_passengers_booked_per_destination.
# Here we are considering both booking and destination data to find which destination doesn't have any booking.

# While running this file in aws lambda, please increase the memory size upto atleast upto 1024 MB 
# and also create a layer to add the package pandas by clicking on the add layer button.

import os
from datetime import date
import boto3

import pandas as pd

def lambda_handler(event, context):
    # AWS S3 details
    aws_access_key = os.getenv('ACCESS_KEY')
    aws_secret_key= os.getenv('SECRET_KEY')
    bucket_name = os.getenv('S3_BUCKET_NAME')

    # Set up the AWS S3 client
    s3 = boto3.client('s3', 
                  aws_access_key_id=aws_access_key, 
                  aws_secret_access_key=aws_secret_key
                  )
    
    # Search for the directory with the current date as the directory name
    search_directory = str(date.today()) + '/'
    response = s3.list_objects(Bucket=bucket_name, Prefix=search_directory)
    file_list = []
    
    # Return error message if the directory doesn't exist 
    if not 'Contents' in response:
        return("Searched directory path doesn't exist")
        
    for obj in response['Contents']:
        # Exclude directories and files with invalid extensions
        if not obj['Key'].endswith('/') and obj['Key'].endswith('.csv'):
            file_list.append(obj['Key'])
            
        
    booking_df = []
    destination_df = []
    
    #Group csv files of booking and destination in seperate lists
    for file in file_list:
        if 'booking' in file:
            obj = s3.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(obj['Body'], compression='gzip')
            booking_df.append(df)
        elif 'destination' in file:
            obj = s3.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(obj['Body'], compression='gzip')
            destination_df.append(df)
        else:
            pass
    
    #Return error if booking or destination files doesn't exist
    if len(booking_df)==0 or len(destination_df)==0:
        return("Booking or destination data doesn't exist")
    
    #Concatenate dataframes of same table if there are more than one
    booking_df = pd.concat(booking_df, ignore_index=True) if len(booking_df) > 1 else booking_df[0]
    destination_df = pd.concat(destination_df, ignore_index=True) if len(destination_df)>1 else destination_df[0]
    
    # First calculating metrics like total_bookings_per_destination, 
    # total_booking_value_per_destination, total_passengers_booked_per_destination 
    booking_agg = booking_df.groupby('destination').agg(
        total_bookings_per_destination=('booking_id', 'count'),
        total_booking_value_per_destination=('total_booking_value', 'sum'),
        total_passengers_booked_per_destination=('number_of_passengers', 'sum')
    )
    
    # destination data left join aggregated booking data
    dest = destination_df.merge(booking_agg, left_on='destination', right_index=True, how='left')
    
    dest[['total_bookings_per_destination',
        'total_booking_value_per_destination',
        'total_passengers_booked_per_destination']]  = dest[['total_bookings_per_destination', 
                                                             'total_booking_value_per_destination', 
                                                             'total_passengers_booked_per_destination']].fillna(0).astype(int)
                                                             
    dest = dest[['destination', 
                 'total_bookings_per_destination', 
                 'total_booking_value_per_destination', 
                 'total_passengers_booked_per_destination']]
                 
    dest = dest.sort_values(by='destination', ascending=True)
    return(dest.set_index('destination', drop=True))