# This script batch-transfers data from the given postgreSQL database to a cloud storage solution(like AWS S3)
import gzip
import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Function to join the tables booking and destination on column destination_id
def get_data_frame(table1, table2):
    df1 = pd.read_sql_table(table1, conn)
    df2 = pd.read_sql_table(table2, conn)
    joined_df = df1.join(df2.set_index(['destination_id']), on='destination_id', how='inner', lsuffix="_x", rsuffix="_y")
    joined_df = joined_df.drop(columns=['destination_id', 'country', 'popular_season'])
    return joined_df.sort_values(by='booking_id', ascending=True)


load_dotenv()

# Database connection url
db_url = os.getenv('DB_URL')
engine = create_engine(db_url)

# Connecting to database
conn = engine.connect()

# AWS S3 details
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key= os.getenv('AWS_SECRET_KEY')
bucket_name = os.getenv('S3_BUCKET_NAME')

s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

# Define a list of tables to transfer
tables = ['customer', 'destination', 'booking']

# Desired batch size
desired_batch_size = 50
for i in range (len(tables)):
    if tables[i] == 'booking':
        df = get_data_frame(tables[i], tables[i-1])
    else:    
        df = pd.read_sql_table(tables[i], conn)
    total_records = df.shape[0]
    num_batches =  (total_records//desired_batch_size) + (1 if total_records % desired_batch_size != 0 else 0)
    
    today = date.today()
    objects = s3.list_objects_v2(Bucket=bucket_name)
    bucket_objects = [obj['Key'] for obj in objects['Contents']] if 'Contents' in objects else []
    
    for batch_number in range(num_batches):
        offset =  batch_number * desired_batch_size
        df_in_batch = df.iloc[offset : offset + desired_batch_size] 
        
        # Write the dataframe to a BytesIO object
        buffer = BytesIO()
        with gzip.open(buffer, 'wb') as f:
             f.write(df_in_batch.to_csv(header=True, index=False).encode())
        
        # Reset the buffer position to the beginning
        buffer.seek(0)    
        
        # Upload the data to AWS S3
        file_name = f'{today}/{tables[i]}/{tables[i]}_{today}_batch_{batch_number+1}.csv'
        
        if file_name not in bucket_objects:
            s3.upload_fileobj(buffer, bucket_name, file_name)
            
# Close the database connection                
conn.close()
print("Batch transferred data successfully")