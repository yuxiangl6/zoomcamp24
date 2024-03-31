from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/mage-terraform-gcp-167d58def28f.json"

bucket_name = 'mage-dezoomcamp-project1'
project_id = 'mage-terraform-gcp'

table_name = 'canada_labor_data_2019'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    # partitioned by ref_date i.e. year
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['ref_date'],
        filesystem = gcs
    )
