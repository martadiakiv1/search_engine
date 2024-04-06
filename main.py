from elasticsearch import Elasticsearch, helpers
import requests


es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}])
if not es.ping():
    raise ValueError("Connection to Elasticsearch failed!")



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
def index_data(data):
    actions = [
        {
            "_index": "my-index",
            "_type": "_doc",
            "_source": record
        }
        for record in data
    ]

    helpers.bulk(es, actions)


# Function to search and return ranked results with error handling and exception management
def search(query, index='my-index'):
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^3", "description", "content"],  # boost the title field
                "fuzziness": "AUTO"  # to allow for a bit of fuzziness in the query
            }
        }
    }

    try:
        results = es.search(index=index, body=body)
        return results['hits']['hits']
    except Exception as e:
        print("An error occurred:", e)
        return []


# Example usage
api_endpoint = 'http://dog-api.kinduff.com/api/facts'
data = fetch_data_from_api(api_endpoint)
if data:
    print(data)
    index_data(data)

# Search query example with user input
user_query = input("Enter your search query: ")
search_results = search(user_query)

# Print search results
for result in search_results:
    print(result['_source'])