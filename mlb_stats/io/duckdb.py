import dagster as dg
from pyarrow import fs as pafs


class DuckPondIOManager(dg.IOManager):
    def __init__(self, bucket_name, account_id, client_access_key, client_secret, duckdb, prefix=""):
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

    def _get_s3_url(self, context):
        if context.has_asset_key:
            id = context.get_asset_identifier()
        else:
            id = context.get_identifier()
        return f"s3://{self.bucket_name}/{self.prefix}{'/'.join(id)}.parquet"

    def handle_output(self, context, select_statement: str):
        if select_statement is None:
            return

        if not isinstance(select_statement, str):
            raise ValueError(f"Expected asset to return a SQL; got {select_statement!r}")

        self.duckdb.query(
            f"COPY ({select_statement}) TO '{self._get_s3_url(context)}' (FORMAT PARQUET)"
        )

    def load_input(self, context) -> str:
        return f"SELECT * FROM read_parquet('{self._get_s3_url(context)}')"


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
