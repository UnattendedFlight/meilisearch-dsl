import unittest

import client
import index
from query import Q


class TestQAndMeiliIndex(unittest.TestCase):
    def setUp(self):
        # print("Running test:", self._testMethodName)
        self.client = client.MeiliClient("url", "api_key")
        self.index = self.client.get_index("test_index")
        self.index.delete_all_documents()
        self.index.add_documents([
            {'id': 1, 'name': 'John Simmons', 'age': 28, 'number': 12, 'category': 'a'},
            {'id': 2, 'name': 'Alice', 'age': 21, 'number': 15, 'category': 'b'},
            {'id': 3, 'name': 'Bob', 'age': 35, 'number': 22, 'category': 'c'},
            {'id': 4, 'name': 'Alice', 'age': 35, 'number': 15, 'category': 'b'},
            {'id': 5, 'name': 'Alice', 'age': 19, 'number': 25, 'category': 'a'},
        ])
        self.index.update_filterable_attributes([
            "name",
            "age",
            "number",
            "category"
        ])

    def test_q_negation(self):
        query = ~Q(name="John Simmons")
        query_string = query.to_query_string()
        self.assertEqual(query_string, "name != \"John Simmons\"")

    def test_q_chaining(self):
        query = (Q(name='John') | Q(name='John Simmons') | ~Q(age__gt=30)) & (Q(number__gte=10)
                                                     & Q(number__lte=20)) | Q(category__in=['a', 'b c', 1])
        query_string = query.to_query_string()
        self.assertEqual(
            query_string, "((((name = John OR name = \"John Simmons\") OR age <= 30) AND (number >= 10 AND number <= 20)) OR category IN [a,\"b c\",1])")

    def test_q_operations(self):
        query = Q(name__neq="John")
        query_string = query.to_query_string()
        self.assertEqual(query_string, "name != John")
        
        query = ~Q(name="John Simmons")
        query_string = query.to_query_string()
        self.assertEqual(query_string, "name != \"John Simmons\"")

        query = Q(category__nin=['a', 'b', 'c d'])
        query_string = query.to_query_string()
        self.assertEqual(query_string, "category NOT IN [a,b,\"c d\"]")

    def test_search(self):
        query = "ali"
        filter = Q(age__gt=19)
        results = self.index.search(query, filter)
        self.assertEqual(len(results['hits']), 2)

        filter &= Q(age__lt=30)
        results = self.index.search(query, filter)
        self.assertEqual(len(results['hits']), 1)

        filter |= Q(category="a")
        results = self.index.search("", filter)
        self.assertEqual(len(results['hits']), 3)

    def test_update_document(self):
        self.index.update_document({
            'id': 1,
            'name': 'Johnathan',
        }, 'id')
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results['hits']), 0)

        results = self.index.search("", Q(name="Johnathan"))
        self.assertEqual(len(results['hits']), 1)

        query = Q(name="Johnathan")
        results = self.index.search("", query)
        self.assertEqual(len(results['hits']), 1)

    def test_delete_document(self):
        self.index.delete_document(1)
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results['hits']), 0)

    def test_delete_all_documents(self):
        self.index.delete_all_documents()
        query = Q(name="John")
        results = self.index.search("", query)
        self.assertEqual(len(results['hits']), 0)


if __name__ == "__main__":
    unittest.main()
