import requests
import json

def execute_requests_with_updated_cookies(period_ids):
    # Path to the JSON file
    file_path = 'config/hyattJS.json'
    
    # Load the initial cookies from the JSON file
    with open(file_path, 'r') as file:
        data = json.load(file)
    initial_cookies = data['source']['request']['cookies']

    # Headers for the first request
    first_request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://hyatthotels.us2.blackline.com/HomePages/HomeExec.aspx',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
    }

    # URL for the first request
    first_request_url = "https://hyatthotels.us2.blackline.com/period/selected-period/edit"

    # Headers for the second request
    second_request_headers = {
        'Accept': 'application/json; version=2',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://hyatthotels.us2.blackline.com',
        'Referer': 'https://hyatthotels.us2.blackline.com/Modules/Reconciliations/ExecGrid.aspx',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    # URL and JSON data for the second request
    second_request_url = "https://hyatthotels.us2.blackline.com/api/accountreconciliation/totals"
    json_data = {
        'options': {
            'grouping': 'GLAccountsOnly',
        },
    }

    for period_id in period_ids:
        # Update params for the current period_id
        params = {'periodId': str(period_id)}

        # Execute the first request
        response = requests.get(first_request_url, params=params, cookies=initial_cookies, headers=first_request_headers)

        # Update the cookie in the JSON data with the new cookie from the response
        data['source']['request']['cookies'].update(response.cookies.get_dict())

        # Save the updated JSON data back to the file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

        # Load the updated cookies for the second request
        updated_cookies = data['source']['request']['cookies']

        # Execute the second request
        response = requests.post(second_request_url, cookies=updated_cookies, headers=second_request_headers, json=json_data)
        print(f'Response for period {period_id}:', response.content)

# List of period IDs
period_ids = [156, 144, 132]  # Update this list with your actual period IDs

# Execute the requests
execute_requests_with_updated_cookies(period_ids)
