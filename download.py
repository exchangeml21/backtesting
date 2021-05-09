from google.cloud import storage
import os
import sys

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )

month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
start_dt, end_dt = sys.argv[1:]
start_month, end_month = start_dt.split('_')[0], end_dt.split('_')[0]
start_yr, end_yr = int(start_dt.split('_')[1]), int(end_dt.split('_')[1])
all_months = []
start_month_ind, end_month_ind = month_names.index(start_month), month_names.index(end_month)

if start_yr < end_yr:
    all_months += [month_name + "_" + str(start_yr) for month_name in month_names[start_month_ind:]]
    all_months += [month_name + "_" + str(end_yr) for month_name in month_names[:end_month_ind+1]]
else:
    all_months += [month_name + "_" + str(start_yr) for month_name in month_names[start_month_ind:end_month_ind+1]]

all_months += [month_name + "_" + str(yr) for yr in range(start_yr + 1, end_yr) for month_name in months]
print(all_months)
for month in all_months:
    download_blob('global_data_feed', month+".zip", "/mnt/disks/gdf/historic_data/data/"+month+".zip")
