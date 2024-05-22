from google.cloud import bigquery

class BigQueryChecker:
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.client = bigquery.Client()

    def table_exists(self, table_id):
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_id)
            table = self.client.get_table(table_ref)
            return True
        except Exception as e:
            if 'Not found' in str(e):
                return False
            else:
                return 'Something wrong'
