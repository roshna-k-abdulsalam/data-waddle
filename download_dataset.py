import requests
import os

urls = [
        "https://raw.githubusercontent.com/roshna-k-abdulsalam/travel-booking-dataset/main/customer.csv",
        "https://github.com/roshna-k-abdulsalam/travel-booking-dataset/raw/main/destination.csv",
        "https://raw.githubusercontent.com/roshna-k-abdulsalam/travel-booking-dataset/main/booking.csv"
        ]

def download_files(url, download_path):
    # Get the filename from the URL
    filename = os.path.basename(url)
    
    response = requests.get(url, download_path)
    if response.status_code == 200:
        # Determine the path to save the file in the current working directory
        save_path = os.path.join(download_path, filename)
        
        if os.path.exists(save_path):
            os.remove(save_path)

        # Save the file
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded the file {filename} and saved successfully.")
    else:
        print(f"Error downloading the file {filename}  with status code:  {response.status_code}")


current_path =  os.getcwd()
download_path = os.path.join(os.getcwd(), "download_dataset")
if not os.path.exists(download_path):
    os.makedirs(download_path)

for url in urls:
    download_files(url, download_path)