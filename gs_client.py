from google.cloud import storage
import os

def get_files(bucket_name, source_folder_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix = source_folder_name)
    files = [blob.name for blob in blobs]
    return sorted(files)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    #print(
    #    "File {} uploaded to {}.".format(
    #        source_file_name, destination_blob_name
    #    )
    #)




def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    #print(
    #    "Blob {} downloaded to {}.".format(
    #        source_blob_name, destination_file_name
    #    )
    #)

#upload_blob('global_data_feed', '/home/exchangeml21_gmail_com/dec.zip', 'tick_by_tick/test/dec.zip')
#download_blob('global_data_feed', 'tick_by_tick/test/dec.zip', 'dec.zip')
#download_blob('global_data_feed','dec_19.zip', 'dec.zip')
