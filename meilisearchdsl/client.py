from time import sleep

from meilisearch import Client
from meilisearch.errors import MeiliSearchApiError

from .index import MeiliIndex


class MeiliClient:
    client: Client = None
    config: dict = None
    indexes: dict = {}

    def __init__(self, host: str, master_key: str):
        self.config = {
            'host': host,
            'master_key': master_key
        }
        self.client = Client(host, master_key)
        self.indexes = {}

    def get_client(self) -> Client:
        if self.client is None:
            assert self.config is not None, "MeiliClient config not set"
            assert 'host' in self.config, "MeiliClient config missing host"
            assert 'master_key' in self.config, "MeiliClient config missing master_key"
            try:
                self.client = Client(
                    self.config['host'], self.config['master_key'])
            except MeiliSearchApiError:
                print("MeiliClient failed to connect to MeiliSearch")
                return None
        return self.client

    def get_index(self, index_name: str) -> MeiliIndex:
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        if index_name not in self.indexes:
            self.indexes[index_name] = MeiliIndex(index_name, self.client)
        return self.indexes[index_name]

    def delete_index(self, index_name: str):
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        try:
            if index_name in self.indexes:
                self.indexes[index_name].delete()
                del self.indexes[index_name]
            else:
                self.client.delete_index(index_name)
        except MeiliSearchApiError as e:
            raise Exception(
                "Failed to delete index '%s': %s" % (index_name, e))

    def get_indexes(self) -> list:
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        return self.client.get_indexes()

    def get_health(self) -> dict:
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        return self.client.health()

    def get_stats(self) -> dict:
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        return self.client.get_all_stats()

    def get_version(self) -> dict:
        self.client = self.get_client()
        assert self.client is not None, "No Meilisearch client"

        return self.client.get_version()
