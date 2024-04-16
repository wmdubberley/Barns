import requests,json
from util import createSession

def getPeriodList(session,config_data):
    base_url=config_data['source']['base_url']
    cookies = config_data['source']['request']['cookies']
    periodfile=f"config/{config_data['source']['periodfile']}"
    headers=config_data['source']['request']['headers']

    response = requests.get(f'{base_url}/api/period/list',cookies=cookies,headers=headers)

    if response.status_code == 200:
        # Convert the response content to a JSON object
        # print(response.json())
        data = response.json()
        
       # Save the JSON object to a file (e.g., response.json)
        with open(periodfile, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    
    file_path = 'config/barnes.json'
    with open(file_path, 'r') as file:
        config_data = json.load(file)
    session=createSession()
    getPeriodList(session,config_data)

    