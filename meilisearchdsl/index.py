# pylint: disable=W0719,C0103,R0904,E1131,import-error
"""Index Module"""
from time import sleep
from typing import Any, Dict, List, Optional, Union

import meilisearch
from meilisearch.errors import MeiliSearchApiError
from meilisearch.index import Index
from meilisearch.models.task import TaskInfo, Task

from .query import Q


class MeiliIndex:
    """MeiliIndex class."""

    def __init__(self, index_name: str, client: meilisearch.Client, primary_key: str):
        self.index_name = index_name
        self.client: meilisearch.Client = client
        try:
            self._index: Index = self.get_index(index_name, primary_key)
        except Exception:
            self._index: Index = self.create_index(index_name, primary_key)

    def get_index(
        self,
        index_name: str,
        primary_key: Optional[str] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> Index:
        """Get an index from Meilisearch."""
        assert self.client is not None, "No Meilisearch client"
        try:
            self._index = self.client.get_index(index_name)
        except Exception:
            index_options = {}
            if options is not None:
                assert isinstance(
                    options, dict), "Options must be a dictionary"
            if primary_key is not None:
                index_options["primaryKey"] = primary_key
            if options is not None:
                index_options.update(options)

            self._call_long_index_method(
                self.client.create_index, index_name, index_options
            )

            self._index = self.client.get_index(index_name)
        return self._index
    
    def create_index(
        self,
        index_name: str,
        primary_key: Optional[str] = None,
        options: Union[Dict[str, Any], None] = None,
    ) -> Index:
        """Create an index in Meilisearch."""
        assert self.client is not None, "No Meilisearch client"
        index_options = {}
        if options is not None:
            assert isinstance(options, dict), "Options must be a dictionary"
        if primary_key is not None:
            index_options["primaryKey"] = primary_key
        if options is not None:
            index_options.update(options)

        self._call_long_index_method(
            self.client.create_index, index_name, index_options
        )

        self._index = self.client.get_index(index_name)
        return self._index

    def update_filterable_attributes(self, attributes: List[str]) -> Union[TaskInfo, Task]:
        """Update filterable attributes of the index.

        Parameters
        ----------
        body:
            List containing the filterable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(
            self._index.update_filterable_attributes, attributes
        )

    def aupdate_filterable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update filterable attributes of the index.

        Parameters
        ----------
        body:
            List containing the filterable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_filterable_attributes(attributes)

    def update_searchable_attributes(self, attributes: List[str]) -> Union[TaskInfo, Task]:
        """Update searchable attributes of the index.

        Parameters
        ----------
        body:
            List containing the searchable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(
            self._index.update_searchable_attributes, attributes
        )

    def aupdate_searchable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update searchable attributes of the index.

        Parameters
        ----------
        body:
            List containing the searchable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_searchable_attributes(attributes)

    def update_displayed_attributes(self, attributes: List[str]) -> Union[TaskInfo, Task]:
        """Update displayed attributes of the index.

        Parameters
        ----------
        body:
            List containing the displayed attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(
            self._index.update_displayed_attributes, attributes
        )

    def aupdate_displayed_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update displayed attributes of the index.

        Parameters
        ----------
        body:
            List containing the displayed attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_displayed_attributes(attributes)

    def update_ranking_rules(self, rules: List[str]) -> Union[TaskInfo, Task]:
        """Update ranking rules of the index.

        Parameters
        ----------
        body:
            List containing the ranking rules.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.update_ranking_rules, rules)

    def aupdate_ranking_rules(self, rules: List[str]) -> TaskInfo:
        """Update ranking rules of the index.

        Parameters
        ----------
        body:
            List containing the ranking rules.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a
            task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_ranking_rules(rules)

    def get_settings(self) -> Dict[str, Any]:
        """Get settings of the index.

        https://docs.meilisearch.com/reference/api/settings.html

        Returns
        -------
        settings
            Dictionary containing the settings of the index.

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.get_settings)

    def aget_settings(self) -> Dict[str, Any]:
        """Get settings of the index.

        https://docs.meilisearch.com/reference/api/settings.html

        Returns
        -------
        settings
            Dictionary containing the settings of the index.

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.get_settings()

    def update_settings(self, settings: Dict[str, Any]) -> TaskInfo:
        """Update settings of the index.

        https://docs.meilisearch.com/reference/api/settings.html#update-settings

        Parameters
        ----------
        body:
            Dictionary containing the settings of the index.
            More information:
            https://docs.meilisearch.com/reference/api/settings.html#update-settings

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.update_settings, settings)

    def aupdate_settings(self, settings: Dict[str, Any]) -> TaskInfo:
        """Update settings of the index.

        https://docs.meilisearch.com/reference/api/settings.html#update-settings

        Parameters
        ----------
        body:
            Dictionary containing the settings of the index.
            More information:
            https://docs.meilisearch.com/reference/api/settings.html#update-settings

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_settings(settings)

    def add_documents(
        self,
        documents: List[Dict[str, Any]],
        primary_key: Optional[str] = None,
    ) -> TaskInfo:
        """Add documents to the index.

        Parameters
        ----------
        documents:
            List of documents. Each document should be a dictionary.
        primary_key (optional):
            The primary-key used in index. Ignored if already set up.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(
            self._index.add_documents, documents, primary_key
        )

    def aadd_documents(
        self,
        documents: List[Dict[str, Any]],
        primary_key: Optional[str] = None,
    ) -> TaskInfo:
        """Add documents to the index.

        Parameters
        ----------
        documents:
            List of documents. Each document should be a dictionary.
        primary_key (optional):
            The primary-key used in index. Ignored if already set up.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.add_documents(documents, primary_key)

    def update_document(
        self, document: Dict, primary_key: Optional[str] = None
    ) -> TaskInfo:
        """Update documents in the index.

        Parameters
        ----------
        documents:
            List of documents. Each document should be a dictionary.
        primary_key (optional):
            The primary-key used in index. Ignored if already set up

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a finished task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(
            self._index.update_documents, [document], primary_key
        )

    def aupdate_documents(
        self, documents: List[Dict], primary_key: Optional[str] = None
    ) -> TaskInfo:
        """Update documents in the index.

        Parameters
        ----------
        documents:
            List of documents. Each document should be a dictionary.
        primary_key (optional):
            The primary-key used in index. Ignored if already set up

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a finished task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.update_documents(documents, primary_key)

    def delete_document(self, document_id: Union[str, int]) -> TaskInfo:
        """Delete one document from the index.

        Parameters
        ----------
        document_id:
            Unique identifier of the document.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(self._index.delete_document, document_id)

    def adelete_document(self, document_id: Union[str, int]) -> TaskInfo:
        """Delete one document from the index.

        Parameters
        ----------
        document_id:
            Unique identifier of the document.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.delete_document(document_id)

    def delete_all_documents(self) -> TaskInfo:
        """Delete all documents from the index.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(self._index.delete_all_documents)

    def adelete_all_documents(self) -> TaskInfo:
        """Delete all documents from the index.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track
            the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request.
            Meilisearch error codes are described here:
            https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.delete_all_documents()

    def search(
        self,
        search_string,
        q: Optional[Q] = None,
        opt_params: Union[Dict[str, Any], None] = None,
    ) -> Dict[str, Any]:
        """Search for documents in the index."""
        search_params = {}
        if q is not None:
            assert isinstance(q, Q), "q must be a Q object"
            search_params["filter"] = q.to_query_string()
        if opt_params is not None:
            assert isinstance(
                opt_params, dict), "opt_params must be a dictionary"
            search_params.update(opt_params)

        return self._index.search(search_string, search_params)

    def _await_running_task(self, task_info: TaskInfo) -> Any:
        """Wait for a task to complete and return the task info object."""
        complete = False
        timeout_seconds = 10
        count = 0
        while not complete:
            if count > timeout_seconds:
                print(
                    f"Task '{task_info['type']}:{task_info['taskUid']}'", # type: ignore
                    f"timed out after {timeout_seconds} seconds",
                )
                break
            task = self.client.get_task(task_info.task_uid)
            if task["status"] == "succeeded":
                return task
            if task["status"] == "failed":
                raise Exception(
                    f"Task '{task['type']}' failed: ",
                    task["uid"],
                    task["error"],
                    task["duration"],
                )
            sleep(0.5)
            count += 0.5
        return None

    def _call_long_index_method(self, function, *args, **kwargs) -> Any:
        """Call a method that returns a taskInfo object and wait for the task to complete."""
        task_info: TaskInfo = function(*args, **kwargs)
        return self._await_running_task(task_info)
