from elasticsearch import Elasticsearch, helpers
import requests
import ir_datasets

# Your Elasticsearch cluster credentials
es_host = "localhost"  # or the address of your Elasticsearch cluster
es_port = 9200  # default port for Elasticsearch
es_user = "superuser"  # replace with your actual Elasticsearch username
es_password = "1452"  # replace with your actual Elasticsearch password

# Creating an Elasticsearch client instance with HTTP Basic Authentication and HTTPS
es_client = Elasticsearch(
    hosts=[{"host": es_host, "port": es_port, "scheme": "https"}],
    basic_auth=(es_user, es_password),
)

es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200, "scheme": "https"}])
if not es.ping():
    raise ValueError("Connection to Elasticsearch failed!")

dataset = ir_datasets.load("c4/en-noclean-tr")


# Function to fetch data from API
def fetch_data_from_api(api_endpoint):
    try:
        response = requests.get(api_endpoint)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        return response.json()
    except requests.exceptions.RequestException as e:
        print(e)
        return None


# Function to index data into Elasticsearch using the bulk API
def index_data(dataset, index_name="c4_dataset"):
    index_name = "my-index"  # Replace with your actual index name
    mapping = es.indices.get_mapping(index=index_name)
    print(mapping[index_name]["mappings"])
    actions = (
        {
            "_index": index_name,
            "_type": "_doc",
            "_id": doc.doc_id,
            "_source": {
                "text": doc.text,
                "url": doc.url,
                "timestamp": doc.timestamp,
            },
        }
        for doc in dataset.docs_iter()
    )
    helpers.bulk(es, actions)


index_data(dataset)


# Function to search and return ranked results with error handling and exception management
def search(query, index="c4_dataset"):
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "title^3",
                    "description",
                    "content",
                ],  # boost the title field
                "fuzziness": "AUTO",  # to allow for a bit of fuzziness in the query
            }
        }
    }

    try:
        results = es.search(index=index, body=body)
        return results["hits"]["hits"]
    except Exception as e:
        print("An error occurred:", e)
        return []


# Search query example with user input
user_query = input("Enter your search query: ")
search_results = search(user_query)

# Print search results
for result in search_results:
    print(result["_source"])
