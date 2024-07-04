import google.auth.transport.requests as gar
import google.oauth2.id_token as goi
import requests
from dagster import ConfigurableResource
from pydantic import Field, BaseModel
from requests import Response


class RequestException(Exception):
    pass


class FantasyLoaderResponse(BaseModel):
    message: str
    file_name: str


class FantasyLoader(ConfigurableResource):
    base_url: str = Field(description="The base_url for the Fantasy Loader")

    @staticmethod
    def get_bearer_token(audience) -> str:
        auth_req = gar.Request()
        return goi.fetch_id_token(auth_req, audience)

    def request(self, endpoint: str, query: str) -> Response:
        url = f"{self.base_url}{endpoint}" + (f"?{query}" if query else "")
        headers = {"Authorization": f"Bearer {self.get_bearer_token(self.base_url)}"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise RequestException(f"Request to {url} returned status code {response.status_code}")

        return response
