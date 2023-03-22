""" Basic tests for the MeiliSearch DSL """
import unittest

from meilisearchdsl.client import MeiliClient
from meilisearchdsl.query import Q


class TestQAndMeiliIndex(unittest.TestCase):
    """Test the Q object and MeiliIndex class"""

    def setUp(self):
        # print("Running test:", self._testMethodName)
        self.client = MeiliClient(
            "http://172.25.0.4:7700", "MJPZij#@4o$NgYIRwxoU2aa^CUq9%5Lq"
        )
        self.index = self.client.get_index("test_index")
        self.index.delete_all_documents()
        self.index.add_documents(
            [
                {
                    "id": 1,
                    "name": "John Simmons",
                    "age": 28,
                    "number": 12,
                    "category": "a",
                },
                {"id": 2, "name": "Alice", "age": 21, "number": 15, "category": "b"},
                {"id": 3, "name": "Bob", "age": 35, "number": 22, "category": "c"},
                {"id": 4, "name": "Alice", "age": 35, "number": 15, "category": "b"},
                {"id": 5, "name": "Alice", "age": 19, "number": 25, "category": "a"},
            ]
        )
        self.index.update_filterable_attributes(["name", "age", "number", "category"])

    def test_q_negation(self):
        """Test the negation of Q objects"""
        query = ~Q(name="John Simmons")
        query_string = query.to_query_string()
        self.assertEqual(query_string, 'name != "John Simmons"')

    def test_q_chaining(self):
        """Test the chaining of Q objects"""
        query = (Q(name="John") | Q(name="John Simmons") | ~Q(age__gt=30)) & (
            Q(number__gte=10) & Q(number__lte=20)
        ) | Q(category__in=["a", "b c", 1])
        query_string = query.to_query_string()
        self.assertEqual(
            query_string,
            '((((name = John) OR (name = "John Simmons")) OR (age <= 30)) AND '
            + '((number >= 10) AND (number <= 20))) OR (category IN [a,"b c",1])',
        )

    def test_q_operations(self):
        """Test the different operations of Q objects"""
        query = Q(name__neq="John")
        query_string = query.to_query_string()
        self.assertEqual(query_string, "name != John")

        query = ~Q(name="John Simmons")
        query_string = query.to_query_string()
        self.assertEqual(query_string, 'name != "John Simmons"')

        query = Q(category__nin=["a", "b", "c d"])
        query_string = query.to_query_string()
        self.assertEqual(query_string, 'category NOT IN [a,b,"c d"]')

    def test_search(self):
        """Test the search method of MeiliIndex"""
        query = "ali"
        q_filter = Q(age__gt=19)
        results = self.index.search(query, q_filter)
        self.assertEqual(len(results["hits"]), 2)

        q_filter &= Q(age__lt=30)
        results = self.index.search(query, q_filter)
        self.assertEqual(len(results["hits"]), 1)

        q_filter |= Q(category="a")
        results = self.index.search("", q_filter)
        self.assertEqual(len(results["hits"]), 3)

    def test_update_document(self):
        """Test the update_document method of MeiliIndex"""
        self.index.update_document(
            {
                "id": 1,
                "name": "Johnathan",
            },
            "id",
        )
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results["hits"]), 0)

        results = self.index.search("", Q(name="Johnathan"))
        self.assertEqual(len(results["hits"]), 1)

        query = Q(name="Johnathan")
        results = self.index.search("", query)
        self.assertEqual(len(results["hits"]), 1)

    def test_delete_document(self):
        """Test the delete_document method of MeiliIndex"""
        self.index.delete_document(1)
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results["hits"]), 0)

    def test_delete_all_documents(self):
        """Test the delete_all_documents method of MeiliIndex"""
        self.index.delete_all_documents()
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results["hits"]), 0)


if __name__ == "__main__":
    unittest.main()
