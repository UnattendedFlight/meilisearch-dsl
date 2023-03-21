# MeiliSearch DSL

MeiliSearch DSL is a powerful and easy-to-use Python package that provides a domain-specific language (DSL) for querying [MeiliSearch](https://www.meilisearch.com/), an open-source search engine. With this package, you can effortlessly build complex search queries using a Django-like Q object syntax. It also includes a convenient wrapper for MeiliSearch indexes and client, making it even simpler to interact with your MeiliSearch instance. Whether you're a beginner or an experienced developer, MeiliSearch DSL can streamline your search experience and help you create efficient, readable, and maintainable search queries.

## Features

- Django-like Q object syntax for building complex search queries
- Convenient wrapper for MeiliSearch indexes and clients
- Easy integration with MeiliSearch instances
- Streamlined search interactions
- Improved code readability and maintainability

## Installation

Install the package using pip:
`pip install meilisearch-dsl`

## Usage

To use MeiliSearch DSL, first import the necessary components:

```python
from meilisearch_dsl import Q, MeiliClient, MeiliIndex

# Then, create a MeiliSearch client and index:
client = MeiliClient(host="http://127.0.0.1:7700", master_key="your_master_key")
index = client.get_index("your_index_name")
# or
index = MeiliIndex("your_index_name", client)

# Build a search filter using the Q object syntax:
filter = ( Q(category__in=["programming", "software"])) & ~Q(price__gt=50)

# Search the index using the filter:
query="Python"
results = index.search(query, filter)
print(results)
```

## Documentation
For more detailed information and examples, please refer to the [official documentation](https://github.com/unattendedflight/meilisearch-dsl/wiki).

## Contributing
We welcome contributions from the community! Please read our [contribution guidelines](https://github.com/unattendedflight/meilisearch-dsl/blob/master/CONTRIBUTING.md) before submitting a pull request.

## License
MeiliSearch DSL is licensed under the [MIT License](https://github.com/unattendedflight/meilisearch-dsl/blob/master/LICENSE).
