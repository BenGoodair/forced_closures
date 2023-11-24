#load modules


import http.client
import pandas as pd
import json
import time
from pandas import json_normalize
import time






#####LOCATION API#####

#Task 1 = get all location IDs not care homes
conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=1')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df1 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=2')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df2 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=3')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df3 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=4')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df4 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=5')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df5 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=6')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df6 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=7')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df7 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=8')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df8 = pd.DataFrame(json_data)

conn = http.client.HTTPSConnection('api.cqc.org.uk')
conn.request('GET','/public/v1/locations?careHome=N&perPage=10000&page=9')
res = conn.getresponse()

data = res.read().decode('utf-8')  # Decode the data from bytes to a string
json_data = json.loads(data)       # Parse the JSON data

# Create a pandas DataFrame from the JSON data
df9 = pd.DataFrame(json_data)


df_first_scrape = pd.concat([df1, df2, df3])

df_second_scrape = pd.concat([df6, df5, df4])

df_third_scrape = pd.concat([df7, df8, df9])

#save all carehomes
#csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/all_carehomes_all_info.csv'

# Use the to_csv method to write the DataFrame to a CSV file
#result_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column

#extract all the IDs

# Function to extract locationId
def extract_location_id(location_dict):
    return location_dict.get('locationId')

# Apply the function to the 'locations' column
df_first_scrape['locationId'] = df_first_scrape['locations'].apply(extract_location_id)
df_second_scrape['locationId'] = df_second_scrape['locations'].apply(extract_location_id)
df_third_scrape['locationId'] = df_third_scrape['locations'].apply(extract_location_id)














#15:57 start, 17:02 finish = 65 minutes to run this code - until line 139 - where it says end_time
#new pb = 54 mins

start_time = time.time()


# Create an empty list to store the results
result_data = []

# Loop through each 'locationId' in your DataFrame
for location_id in df_first_scrape['locationId']:
    while True:
        try:
            conn = http.client.HTTPSConnection('api.cqc.org.uk')
            conn.request('GET', f'/public/v1/locations/{location_id}')
            res = conn.getresponse()

            if res.status == 200:
                data = res.read().decode('utf-8')  # Decode the data from bytes to a string
                json_data = json.loads(data)       # Parse the JSON data

                # Append the data for the current locationId to the result list
                result_data.append(json_data)
                break  # Successfully received data, exit the retry loop

            elif res.status == 429:
                # If API rate limit exceeded, wait and then retry
                print(f"API rate limit exceeded for locationId {location_id}. Waiting and retrying...")
                time.sleep(5)  # Wait five seconds before retrying - longer pause = fewer failures, but chatgpt said do a whole minute of waiting and fuck that

            else:
                # Handle other HTTP status codes as needed
                print(f"Failed to retrieve data for locationId {location_id} with status code {res.status}")
                break  # Exit the retry loop if it's not a rate limit issue or other recoverable error

        except Exception as e:
            print(f"An error occurred for locationId {location_id}: {str(e)}")
            break  # Exit the retry loop on any exception

# Create a DataFrame from the list of JSON data
result_df = pd.DataFrame(result_data)

# Now result_df contains data for all 'locationId' values, and it has handled rate limiting and errors

#save all carehomes
csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/GitHub/forced_closures/Data/not_carehomes_1.csv'

# Use the to_csv method to write the DataFrame to a CSV file
result_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column

print("--- %s seconds ---" % (time.time() - start_time))
end_time = time.time()





#15:57 start, 17:02 finish = 65 minutes to run this code - until line 139 - where it says end_time
#new pb = 54 mins

start_time = time.time()


# Create an empty list to store the results
result_data = []

# Loop through each 'locationId' in your DataFrame
for location_id in df_second_scrape['locationId']:
    while True:
        try:
            conn = http.client.HTTPSConnection('api.cqc.org.uk')
            conn.request('GET', f'/public/v1/locations/{location_id}')
            res = conn.getresponse()

            if res.status == 200:
                data = res.read().decode('utf-8')  # Decode the data from bytes to a string
                json_data = json.loads(data)       # Parse the JSON data

                # Append the data for the current locationId to the result list
                result_data.append(json_data)
                break  # Successfully received data, exit the retry loop

            elif res.status == 429:
                # If API rate limit exceeded, wait and then retry
                print(f"API rate limit exceeded for locationId {location_id}. Waiting and retrying...")
                time.sleep(5)  # Wait five seconds before retrying - longer pause = fewer failures, but chatgpt said do a whole minute of waiting and fuck that

            else:
                # Handle other HTTP status codes as needed
                print(f"Failed to retrieve data for locationId {location_id} with status code {res.status}")
                break  # Exit the retry loop if it's not a rate limit issue or other recoverable error

        except Exception as e:
            print(f"An error occurred for locationId {location_id}: {str(e)}")
            break  # Exit the retry loop on any exception

# Create a DataFrame from the list of JSON data
result_df = pd.DataFrame(result_data)

# Now result_df contains data for all 'locationId' values, and it has handled rate limiting and errors

#save all carehomes
csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/GitHub/forced_closures/Data/not_carehomes_2.csv'

# Use the to_csv method to write the DataFrame to a CSV file
result_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column

print("--- %s seconds ---" % (time.time() - start_time))
end_time = time.time()






#15:57 start, 17:02 finish = 65 minutes to run this code - until line 139 - where it says end_time
#new pb = 54 mins

start_time = time.time()


# Create an empty list to store the results
result_data = []

# Loop through each 'locationId' in your DataFrame
for location_id in df_third_scrape['locationId']:
    while True:
        try:
            conn = http.client.HTTPSConnection('api.cqc.org.uk')
            conn.request('GET', f'/public/v1/locations/{location_id}')
            res = conn.getresponse()

            if res.status == 200:
                data = res.read().decode('utf-8')  # Decode the data from bytes to a string
                json_data = json.loads(data)       # Parse the JSON data

                # Append the data for the current locationId to the result list
                result_data.append(json_data)
                break  # Successfully received data, exit the retry loop

            elif res.status == 429:
                # If API rate limit exceeded, wait and then retry
                print(f"API rate limit exceeded for locationId {location_id}. Waiting and retrying...")
                time.sleep(5)  # Wait five seconds before retrying - longer pause = fewer failures, but chatgpt said do a whole minute of waiting and fuck that

            else:
                # Handle other HTTP status codes as needed
                print(f"Failed to retrieve data for locationId {location_id} with status code {res.status}")
                break  # Exit the retry loop if it's not a rate limit issue or other recoverable error

        except Exception as e:
            print(f"An error occurred for locationId {location_id}: {str(e)}")
            break  # Exit the retry loop on any exception

# Create a DataFrame from the list of JSON data
result_df = pd.DataFrame(result_data)

# Now result_df contains data for all 'locationId' values, and it has handled rate limiting and errors

#save all carehomes
csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/GitHub/forced_closures/Data/not_carehomes_3.csv'

# Use the to_csv method to write the DataFrame to a CSV file
result_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column

print("--- %s seconds ---" % (time.time() - start_time))
end_time = time.time()











