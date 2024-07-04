# Import python packages

import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from dagster import (ConfigurableResource, EnvVar)
from pydantic import Field


class Cloudflare(ConfigurableResource):
    bucket: str = Field(description="The bucket name in the Cloudflare storage",
                        default=EnvVar("CLOUDFLARE_BUCKET"))
    account_id: str = Field(description="The account id in the Cloudflare storage",
                            default=EnvVar("CLOUDFLARE_ACCOUNT_ID"))
    client_access_key: str = Field(description="The client access key in the Cloudflare storage",
                                   default=EnvVar("CLOUDFLARE_CLIENT_ACCESS_KEY"))
    client_secret: str = Field(description="The client secret in the Cloudflare storage",
                               default=EnvVar("CLOUDFLARE_CLIENT_SECRET"))

    @property
    def _connection(self):
        _connection_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
        return pafs.S3FileSystem(
            access_key=self.client_access_key,
            secret_key=self.client_secret,
            endpoint_override=_connection_url
        )

    def put(self, file_name: str, data: pd.DataFrame, metadata=None):
        """
        Put the data into the Cloudflare storage

        :param file_name:
        :param data:
        :return:
        """

        s3 = self._connection
        table = pa.Table.from_pandas(data, preserve_index=False)
        pq.write_table(
            table=table,
            where=f'{self.bucket}/{file_name}',
            filesystem=s3
        )

        return {
            "message": "file_loaded",
            "file_name": f'{file_name}',
        }

    def get(self, file_name: str) -> pd.DataFrame:
        """
        Get the data from the Cloudflare storage

        :param file_name:
        :return:
        """
        s3 = self._connection
        table = pq.read_table(f'{self.bucket}/{file_name}', filesystem=s3)

        return table.to_pandas()

    def check(self, file_name: str) -> bool:
        """
        Check if the file exists in the Cloudflare storage

        :param file_name:
        :return:
        """
        s3 = self._connection
        # if exists then return True else False pyarrow
        file_info = s3.get_file_info(f'{self.bucket}/{file_name}')

        if file_info.type == pafs.FileType.File:
            return True
        else:
            return False
