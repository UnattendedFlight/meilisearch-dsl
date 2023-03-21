import meilisearch
from meilisearch.index import Index
from typing import List, Dict, Any, Optional
from query import Q
from meilisearch.errors import MeiliSearchApiError
from meilisearch.models.task import TaskInfo
from time import sleep

class MeiliIndex:
    def __init__(self, index_name: str, client: meilisearch.Client):
        self.index_name = index_name
        self.client: meilisearch.Client = client
        self._index: Index = None
        self.get_index(index_name)

    def get_index(self, index_name: str, primary_key: str = None, options: Dict[str, Any] | None = None) -> Index:
        assert self.client is not None, "No Meilisearch client"
        try:
            self._index = self.client.get_index(index_name)
        except MeiliSearchApiError as e:
            index_options = {}
            if options is not None:
                assert isinstance(options, dict), "Options must be a dictionary"
            if primary_key is not None:
                index_options["primaryKey"] = primary_key
            if options is not None:
                index_options.update(options)
            
            self._call_long_index_method(self._index.create_index, index_name, index_options)

            self._index = self.client.get_index(index_name)
        return self._index
    
    def update_filterable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update filterable attributes of the index.

        Parameters
        ----------
        body:
            List containing the filterable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.update_filterable_attributes, attributes)
    
    def aupdate_filterable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update filterable attributes of the index.

        Parameters
        ----------
        body:
            List containing the filterable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_filterable_attributes(attributes)
    
    def update_searchable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update searchable attributes of the index.

        Parameters
        ----------
        body:
            List containing the searchable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.update_searchable_attributes, attributes)
    
    def aupdate_searchable_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update searchable attributes of the index.

        Parameters
        ----------
        body:
            List containing the searchable attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_searchable_attributes(attributes)
    
    def update_displayed_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update displayed attributes of the index.

        Parameters
        ----------
        body:
            List containing the displayed attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.update_displayed_attributes, attributes)
    
    def aupdate_displayed_attributes(self, attributes: List[str]) -> TaskInfo:
        """Update displayed attributes of the index.

        Parameters
        ----------
        body:
            List containing the displayed attributes.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.update_displayed_attributes(attributes)
    
    def update_ranking_rules(self, rules: List[str]) -> TaskInfo:
        """Update ranking rules of the index.

        Parameters
        ----------
        body:
            List containing the ranking rules.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
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
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
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
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.get_settings)
        return self._index.get_settings()
    
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
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
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
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
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
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
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
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._call_long_index_method(self._index.add_documents, documents, primary_key)
    
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
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        assert self._index is not None, "No Meilisearch index"
        return self._index.add_documents(documents, primary_key)

    def update_document(self, document: Dict, primary_key: str | None = None) -> TaskInfo:
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
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(self._index.update_documents, [document], primary_key)
    
    def aupdate_documents(self, documents: List[Dict], primary_key: str | None = None) -> TaskInfo:
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
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.update_documents(documents, primary_key)

    def delete_document(self, document_id: str) -> TaskInfo:
        """Delete one document from the index.

        Parameters
        ----------
        document_id:
            Unique identifier of the document.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(self._index.delete_document, document_id)

    def adelete_document(self, document_id: str) -> TaskInfo:
        """Delete one document from the index.

        Parameters
        ----------
        document_id:
            Unique identifier of the document.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.delete_document(document_id)
    
    def delete_all_documents(self) -> TaskInfo:
        """Delete all documents from the index.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._call_long_index_method(self._index.delete_all_documents)
    
    def adelete_all_documents(self) -> TaskInfo:
        """Delete all documents from the index.

        Returns
        -------
        task_info:
            TaskInfo instance containing information about a task to track the progress of an asynchronous process.
            https://docs.meilisearch.com/reference/api/tasks.html#get-one-task

        Raises
        ------
        MeiliSearchApiError
            An error containing details about why Meilisearch can't process your request. Meilisearch error codes are described here: https://docs.meilisearch.com/errors/#meilisearch-errors
        """
        return self._index.delete_all_documents()

    def search(self, search_string, q: Q | None = None, opt_params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        search_params = {}
        if q is not None:
            assert isinstance(q, Q), "q must be a Q object"
            search_params["filter"] = q.to_query_string()
        if opt_params is not None:
            assert isinstance(opt_params, dict), "opt_params must be a dictionary"
            search_params.update(opt_params)

        return self._index.search(search_string, search_params)

    def _await_running_task(self, taskInfo: Dict[str, Any]):
        complete = False
        timeout_seconds = 10
        count = 0
        while not complete:
            if count > timeout_seconds:
                print(
                    f"Task '{taskInfo['type']}:{taskInfo['taskUid']}' timed out after {timeout_seconds} seconds")
                break
            task = self.client.get_task(taskInfo.task_uid)
            if task["status"] == 'succeeded':
                return task
            if task["status"] == 'failed':
                raise Exception(
                    f"Task '{task['type']}' failed: ", task["uid"], task["error"], task["duration"])
            sleep(0.5)
            count += 0.5

    def _call_long_index_method(self, function, *args, **kwargs):
        assert self._index is not None, "No Meilisearch index"
        taskInfo = function(*args, **kwargs)
        return self._await_running_task(taskInfo)
