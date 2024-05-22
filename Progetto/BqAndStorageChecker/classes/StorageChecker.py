from google.cloud import storage


class StorageChecker:
    def __init__(self):
        self.client = storage.Client()

    def blob_exists(self, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()





