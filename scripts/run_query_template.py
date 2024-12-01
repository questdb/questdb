import requests

resp = requests.get(
    'http://localhost:9000/exp',
    {
        'query': 'SELECT * FROM table1',
        'limit': '3,6'   # Rows 3, 4, 5
    })

print(resp.text)