import re

def optimize_query(query):

    pattern = r'SELECT\s*(.*?)\s*FROM\s*(.*?)\s*WHERE\s*(.*?)\s*ORDER BY\s*(.*?)\s*DESC\s*LIMIT\s*(\d+)'

    match = re.search(pattern, query, re.IGNORECASE)

    

    if match:

        columns = match.group(1)

        table = match.group(2)

        conditions = match.group(3)

        order_by = match.group(4)

        limit = int(match.group(5))

        

        if limit > 0:

            optimized_query = f"SELECT {columns} FROM {table} WHERE {conditions} ORDER BY {order_by} DESC LIMIT {limit}"

        else:

            optimized_query = f"SELECT {columns} FROM {table} WHERE {conditions} LIMIT {-limit}"

        

        return optimized_query

    

    return query

# Example usage

original_query = "SELECT column1, column2 FROM my_table WHERE condition = 'value' ORDER BY timestamp DESC LIMIT 10"

optimized_query = optimize_query(original_query)

print(optimized_query)

