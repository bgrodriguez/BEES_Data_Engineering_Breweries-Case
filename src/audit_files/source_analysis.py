import requests

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

page = 1
per_page = 200
total = 0

while True:
    response = requests.get(
        BASE_URL,
        params={"page": page, "per_page": per_page},
        timeout=10
    )
    
    data = response.json()
    
    if not data:
        break
    
    total += len(data)
    print(f"Página {page}: {len(data)} registros")
    
    page += 1

print("Total estimado:", total)