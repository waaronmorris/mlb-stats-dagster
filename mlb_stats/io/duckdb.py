from concurrent.futures import ThreadPoolExecutor, as_completed

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
            endpoint_override=_connection_url,
            request_timeout=3600,
            connect_timeout=3600
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

        # Convert 'Timestamp' columns to ISO 8601 string format
        obj = obj.copy()  # Make a copy to avoid modifying the original dataframe
        for column in obj.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
            obj[column] = obj[column].map(lambda x: x.isoformat())

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
        if context.has_partition_key or context.has_asset_partitions:
            context.log.info(
                f"Reading {context.asset_key} | {len(context.asset_partition_keys)} | {context.has_partition_key} | {context.has_asset_partitions}")
            asset_partitions_def = context.asset_partitions_def
            if len(context.asset_partition_keys) >= 1:

                asset_partitions_def = context.asset_partitions_def
                partition_keys = context.asset_partition_keys
                max_workers = 10
                batch_size = 50

                def load_batch(batch_start, batch_end):
                    batch_df = pd.DataFrame()
                    for i in range(batch_start, batch_end):
                        partition_key = partition_keys[i]
                        file_path = '/'.join(
                            [self.bucket_name, *context.asset_key.path, partition_key.replace('-', '')])
                        try:
                            _partition_df = pq.read_table(
                                source=f'{file_path}.parquet',
                                filesystem=self._connection,
                            ).to_pandas()

                            batch_df = pd.concat([
                                batch_df,
                                _partition_df.astype(str)
                            ])
                        except FileNotFoundError:
                            context.log.warning(f"File {file_path}.parquet not found")
                        except Exception as e:
                            context.log.error(f"Error reading {file_path}.parquet: {e} | {type(e)}")
                    return batch_df

                """ 
                    Load the partitions in parallel
                  """

                num_partitions = len(context.asset_partition_keys)

                # Use ThreadPoolExecutor to process batches in parallel
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for batch_start in range(0, num_partitions, batch_size):
                        batch_end = min(batch_start + batch_size, num_partitions)
                        context.log.info(f"Submitting batch {batch_start} to {batch_end - 1} of {num_partitions}")
                        futures.append(executor.submit(load_batch, batch_start, batch_end))

            # Collect results as each batch completes
            for future in as_completed(futures):
                df = pd.concat([df, future.result()])
        else:
            context.log.info(f"Reading {context.asset_key}")
            file_path = self.__get_path(context)
            try:
                df = pq.read_table(
                    source=f'{file_path}.parquet',
                    filesystem=self._connection
                ).to_pandas()
            except FileNotFoundError as e:
                context.log.warning(f"File {file_path}.parquet not found")
            except Exception as e:
                # Log the Exception Type
                context.log.error(f"Error reading {file_path}.parquet: {e} | {type(e)}")
                df = pd.DataFrame()

        return df.astype(object).convert_dtypes(infer_objects=True)


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
