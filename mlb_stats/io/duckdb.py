import dagster as dg
import pandas as pd
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

    def __get_path(self, context):
        __path = [self.bucket_name, *context.asset_key.path]

        if context.has_partition_key:
            __path.append(context.partition_key.replace('-', ''))

        return '/'.join(__path)

    def handle_output(self, context, obj):
        """
        Handle the output of the asset and store it in the S3 bucket as a parquet file
        :param context:
        :param obj:  table to store in the S3 bucket
        :return:
        """

        if not isinstance(obj, pd.DataFrame):
            return

        file_path = self.__get_path(context)

        r2 = self._connection
        table = pa.Table.from_pandas(
            df=obj,
            preserve_index=False
        )

        pq.write_table(
            table=table,
            where=f'{file_path}.parquet',
            filesystem=r2
        )

    def load_input(self, context: dg.InputContext):
        df = pd.DataFrame()
        asset_partitions_def = context.asset_partitions_def
        if len(context.asset_partition_keys) > 1:
            for partition_key in context.asset_partition_keys:
                file_path = '/'.join([self.bucket_name, *context.asset_key.path, partition_key.replace('-', '')])
                try:
                    df = pd.concat([
                        df,
                        pq.read_table(
                            source=f'{file_path}.parquet',
                            filesystem=self._connection
                        ).to_pandas()])
                except Exception as e:
                    context.log.error(f"Error reading {file_path}.parquet: {e}")
                    df = pd.concat([
                        df,
                        pd.DataFrame()])
        return df



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
