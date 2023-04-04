# pylint: disable=W0719,C0103,R0904, import-error
"""MeiliClient is a wrapper around the MeiliSearch client. It provides a
simple interface to create indexes and add documents to them. It also
provides a simple interface to delete indexes and documents from them.
"""
from typing import Dict, List

from meilisearch import Client
from .index import Index

from .index import MeiliIndex


class MeiliClient:
    """MeiliClient is a wrapper around the MeiliSearch client. It provides a
    simple interface to create indexes and add documents to them. It also
    provides a simple interface to delete indexes and documents from them.
    """

    client: Client
    config: dict
    indexes: dict = {}

    def __init__(self, host: str, master_key: str):
        self.config = {"host": host, "master_key": master_key}
        self.client = Client(host, master_key)
        self.indexes = {}

    def get_client(self) -> Client:
        """Get the MeiliSearch client. If it doesn't exist, create it."""
        if self.client is None:
            assert self.config is not None, "MeiliClient config not set"
            assert "host" in self.config, "MeiliClient config missing host"
            assert "master_key" in self.config, "MeiliClient config missing master_key"
            self.client = Client(
                self.config["host"], self.config["master_key"])
        return self.client

    def get_index(self, index_name: str, primary_key: str) -> MeiliIndex:
        """Get an index by name. If it doesn't exist, create it."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        if index_name not in self.indexes:
            self.indexes[index_name] = MeiliIndex(index_name, self.client, primary_key)
        return self.indexes[index_name]

    def delete_index(self, index_name: str):
        """Delete an index by name."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        if index_name in self.indexes:
            self.indexes[index_name].delete()
            del self.indexes[index_name]
        else:
            self.client.delete_index(index_name)

    def get_indexes(self) -> Dict[str, List[Index]]:
        """Get a list of all indexes."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        return self.client.get_indexes()

    def get_health(self) -> dict:
        """Get the health of the MeiliSearch instance."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        return self.client.health()

    def get_stats(self) -> dict:
        """Get the stats of the MeiliSearch instance."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        return self.client.get_all_stats()

    def get_version(self) -> dict:
        """Get the version of the MeiliSearch instance."""
        self.client = self.get_client()
        assert self.client is not None, ModuleNotFoundError(
            "No Meilisearch client")

        return self.client.get_version()
