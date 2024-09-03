import csv
import io
import os
import requests

from functools import lru_cache


api_username = os.environ.get('NEXTPERTISE_API_USERNAME')
api_password = os.environ.get('NEXTPERTISE_API_PASSWORD')


@lru_cache(maxsize=1)
def get_access_token():
    response = requests.get('https://api.nextpertise.nl/jwt/log-in',
                            auth=(api_username, api_password))
    try:
        data = response.json()
        return data['access_token']
    except KeyError:
        raise ValueError('Invalid credentials: {}'.format(response.text))


def get_active_mobile_connections(debtor_code, page_size=5):
    request_headers = {
        'accept': 'application/json, text/plain, */*',
        'authorization': f'Bearer {get_access_token()}'
    }

    query = f'is_active:true'
    if debtor_code:
        query += f' AND organization.debtor_code:{debtor_code}*'

    all_connections = []
    page = 1
    while True:
        response = requests.get(
            f'https://api.nextpertise.nl/mobile-broadband/connections/?page_size={page_size}&page={page}&query={query}',
            headers=request_headers
        )
        response.raise_for_status()  # This will raise an error for bad responses
        data = response.json()

        # Assuming the data contains a list of connections in a key called 'results'
        connections = data.get('results', [])
        if not connections:
            break

        all_connections.extend(connections)

        # Check if there's a next page or if we've fetched all the data
        if len(connections) < page_size:
            break
        page += 1

    return all_connections


def get_month_to_date_usage(connection_uuid, billing_cycle=None):
    request_headers = {
        'accept': 'application/json, text/plain, */*',
        'authorization': f'Bearer {get_access_token()}'
    }
    url = f'https://api.nextpertise.nl/mobile-broadband/connections/{connection_uuid}/usage/month-to-date/'
    if billing_cycle:
        url += f'?billing_cycle={billing_cycle}'
    response = requests.get(url, headers=request_headers)
    return response.json()


# Please note that the first supported billing cycle is '2024-06-01'
billing_cycle = '2024-06-01'
debtor_code = None

active_mobile_connections = get_active_mobile_connections(debtor_code)
csv_header = ['uuid', 'nid', 'imsi', 'iccid', 'tags', 'usage_in_bytes', 'i18n_usage', 'sms_usage']
output = io.StringIO()
writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC, delimiter=';')
writer.writerow(csv_header)

for connection in active_mobile_connections:
    usage = get_month_to_date_usage(connection['uuid'], billing_cycle=billing_cycle)
    csv_row = [connection['uuid'],
               connection['carrier']['nid'],
               connection['carrier']['imsi'],
               connection['carrier']['sim']['iccid'],
               connection['carrier']['tags'],
               usage['data']['usage_in_bytes'],
               usage['data']['i18n_usage'],
               usage['sms']['usage']
               ]
    writer.writerow(csv_row)

# Write the CSV file
filename = f"mobile_connection_details"
if debtor_code:
    filename += f"_{debtor_code.lower()}"
with open(f"{filename}.csv", "wb") as f:
    f.write(output.getvalue().encode('utf-8'))
