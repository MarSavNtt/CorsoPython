import os

credentials_path = "credentials/name"

# Imposta la variabile d'ambiente GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Ora puoi importare e utilizzare le classi per il controllo di BigQuery e Cloud Storage
from classes.BqChecker import BigQueryChecker
from classes.StorageChecker import StorageChecker

def main():
    bucket_name = 'bucketgcp'
    blob_name = 'company.csv'
    storage_checker = StorageChecker()
    print(storage_checker.blob_exists(bucket_name=bucket_name, blob_name=blob_name))
    dataset_id = 'id-dataset'
    table_id = 'corso_t'
    big_query_checker = BigQueryChecker(dataset_id=dataset_id)
    print(big_query_checker.table_exists(table_id=table_id))




if __name__ == "__main__":
    main()
