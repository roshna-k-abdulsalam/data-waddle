python download_dataset.py
PGPASSWORD=<user-postgresql-password> psql -h <ip/url-of-the-remote-server> -U <user-name> -d <database-name> -w -f travel.sql
python transform_load_csv_data.py
python batch_transfer_data_to_cloud_storage.py