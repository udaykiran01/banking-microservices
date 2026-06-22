import requests
services = [ "http://udaybankingapp.site/health",
            
            "http://udaybankingapp.site/api/health"
            ]

  
#else:    print(f"Health check failed. Status code: {response.status_code}")
for service in services:
    try:
        response = requests.get(service)
        if response.status_code == 200:
            print("Health check successful. The API is up and running.")  
        else:
            print(f"not reachable:{response.status_code}")

    except:
        print("api unreachable")    