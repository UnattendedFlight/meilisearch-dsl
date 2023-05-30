# pylint: disable=R0902
""" The module used for building MultiSearch Index Queries. """
from typing import Dict, List, Optional, Union

from .query import Q


class IndexQuery:
    """
    The class used for translating a MeiliSearchDSL query into a
    MeiliSearch query for use with the multi search endpoint.
    """

    _attributes = {
        "filter": Optional[Q],
        "limit": Optional[int],
        "offset": Optional[int],
        "hits_per_page": Optional[int],
        "page": Optional[int],
        "facets": Optional[List[str]],
        "attributes_to_retrieve": Optional[List[str]],
        "attributes_to_crop": Optional[List[str]],
        "crop_length": Optional[int],
        "crop_marker": Optional[str],
        "attributes_to_highlight": Optional[List[str]],
        "highlight_pre_tag": Optional[str],
        "highlight_post_tag": Optional[str],
        "show_matches_position": Optional[bool],
        "sort": Optional[List[str]],
        "matching_strategy": Optional[str],
    }

    def __init__(self, index_uid: str, search_query: str):
        self.index_uid = index_uid
        self.search_query = search_query

        # Initialize attributes to None
        for attr in self._attributes:
            setattr(self, attr, None)

    def get_dict(self) -> Dict[str, Union[str, int, List[str]]]:
        """
        Return a dictionary representation of the query.
        """
        return_dict: Dict[str, Union[str, int, List[str]]] = {
            "indexUid": self.index_uid,
            "q": self.search_query,
        }
        for attr, attr_type in self._attributes.items():
            value = getattr(self, attr)
            if value is not None:
                return_dict[attr] = value if attr_type != Q else value.to_query_string()
        return return_dict

    def set_attr(self, attr: str, value: Union[str, int, List[str], Q]):
        """
        Set an attribute of the query.
        """
        if attr not in self._attributes:
            raise AttributeError(f"Attribute {attr} does not exist.")
        setattr(self, attr, value)
        return self


class IndexSearch:
    """The class used for building MultiSearch Index Queries."""

    def __init__(
        self,
        index_uid: str,
        search: str,
    ):
        self.index_query = IndexQuery(index_uid, search)

    def filter(self, _filter: Q):
        """Set the filter for the query."""
        self.index_query.set_attr("filter", _filter)
        return self

    def limit(self, limit: int):
        """Set the limit for the query."""
        self.index_query.set_attr("limit", limit)
        return self

    def offset(self, offset: int):
        """Set the offset for the query."""
        self.index_query.set_attr("offset", offset)
        return self

    def hits_per_page(self, hits_per_page: int):
        """Set the hits per page for the query."""
        self.index_query.set_attr("hits_per_page", hits_per_page)
        return self

    def page(self, page: int):
        """Set the page for the query."""
        self.index_query.set_attr("page", page)
        return self

    def facets(self, facets: List[str]):
        """Set the facets for the query."""
        self.index_query.set_attr("facets", facets)
        return self

    def retrieve_attributes(self, attributes_to_retrieve: List[str]):
        """Set the attributes to retrieve for the query."""
        self.index_query.set_attr("attributes_to_retrieve", attributes_to_retrieve)
        return self

    def crop_attributes(self, attributes_to_crop: List[str]):
        """Set the attributes to crop for the query."""
        self.index_query.set_attr("attributes_to_crop", attributes_to_crop)
        return self

    def crop_length(self, crop_length: int):
        """Set the crop length for the query."""
        self.index_query.set_attr("crop_length", crop_length)
        return self

    def crop_marker(self, crop_marker: str):
        """Set the crop marker for the query."""
        self.index_query.set_attr("crop_marker", crop_marker)
        return self

    def highlight_attributes(self, attributes_to_highlight: List[str]):
        """Set the attributes to highlight for the query."""
        self.index_query.set_attr("attributes_to_highlight", attributes_to_highlight)
        return self

    def highlight_pre_tag(self, highlight_pre_tag: str):
        """Set the highlight pre tag for the query."""
        self.index_query.set_attr("highlight_pre_tag", highlight_pre_tag)
        return self

    def highlight_post_tag(self, highlight_post_tag: str):
        """Set the highlight post tag for the query."""
        self.index_query.set_attr("highlight_post_tag", highlight_post_tag)
        return self

    def show_matches_position(self, show_matches_position: bool):
        """Set the show matches position for the query."""
        self.index_query.set_attr("show_matches_position", show_matches_position)
        return self

    def sort(self, sort: List[str]):
        """Set the sort for the query."""
        self.index_query.set_attr("sort", sort)
        return self

    def matching_strategy(self, matching_strategy: str):
        """Set the matching strategy for the query."""
        self.index_query.set_attr("matching_strategy", matching_strategy)
        return self

    def query(self):
        """Return the query."""
        return self.index_query.get_dict()
