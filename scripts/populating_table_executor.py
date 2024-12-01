import requests

# Define the QuestDB HTTP API endpoint
QUESTDB_URL = "http://localhost:9000/exec"  # Replace with the correct endpoint if different

# Function to read queries from a file
def read_queries_from_file(file_path):
    with open(file_path, 'r') as file:
        queries = file.read().strip().split(';')
        # Remove any empty queries and append a semicolon to each
        return [query.strip() + ';' for query in queries if query.strip()]

# Function to execute queries sequentially
def execute_queries(queries, url):
    for query in queries:
        try:
            response = requests.get(url, params={'query': query})
            if response.status_code == 200:
                print(f"Query executed successfully: {query}")
            else:
                print(f"Failed to execute query: {query}")
                print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Exception: {e}")

# Main script
if __name__ == "__main__":
    # File containing SQL queries (one query per line)
    QUERY_FILE = "table_creation.txt"  # Replace with your file path

    # Read queries from the file
    queries = read_queries_from_file(QUERY_FILE)

    # Execute the queries
    execute_queries(queries, QUESTDB_URL)