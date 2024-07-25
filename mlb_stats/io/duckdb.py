import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs


class DuckPondIOManager(dg.IOManager):
    def __init__(self,
                 bucket_name,
                 account_id,
                 client_access_key,
                 client_secret,
                 duckdb,
                 prefix=""):
        self.bucket_name = bucket_name
        self.duckdb = duckdb
        self.account_id = account_id
        self.client_access_key = client_access_key
        self.client_secret = client_secret
        self.prefix = prefix

    @property
    def _connection(self):
        _connection_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
        return pafs.S3FileSystem(
            access_key=self.client_access_key,
            secret_key=self.client_secret,
            endpoint_override=_connection_url
        )

    def _get_r2_url(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"r2://{self.bucket_name}/{self.prefix}{'/'.join(id)}.parquet"

    def handle_output(self, context, obj: "pd.DataFrame"):
        """
        Handle the output of the asset and store it in the S3 bucket as a parquet file
        :param context:
        :param obj:  table to store in the S3 bucket
        :return:
        """

        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()

        r2 = self._connection
        table = pa.Table.from_pandas(df=obj, preserve_index=False)

        pq.write_table(
            table=table,
            where=f'{self.bucket_name}/{self.prefix}{"/".join(id)}.parquet',
            filesystem=r2
        )



    def load_input(self, context) -> str:
        # Load the data from the S3 bucket
        table = self.duckdb.query(
            f"COPY '{self._get_r2_url(context)}' TO '{context}' (FORMAT PARQUET)"
        )


@dg.io_manager
def duckdb_io_manager(init_context):
    return DuckPondIOManager(
        bucket_name=init_context.resource_config['bucket_name'],
        account_id=init_context.resource_config['account_id'],
        client_access_key=init_context.resource_config['client_access_key'],
        client_secret=init_context.resource_config['client_secret'],
        duckdb=init_context.resource_config['duckdb'],
        prefix=init_context.resource_config.get('prefix', '')
    )
