














#15:57 start, 17:02 finish = 65 minutes to run this code - until line 139 - where it says end_time
#new pb = 54 mins

start_time = time.time()


# Create an empty list to store the results
result_data = []

# Loop through each 'locationId' in your DataFrame
for location_id in df['locationId']:
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
#csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/all_carehomes_all_info_full_Oct.csv'

# Use the to_csv method to write the DataFrame to a CSV file
#result_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column

print("--- %s seconds ---" % (time.time() - start_time))
end_time = time.time()



#current ratings!#

currentRatings_df = json_normalize(result_df['currentRatings'])

# Combine the 'locationID' column with the flattened data
currentRatings_df = pd.concat([result_df['locationId'], currentRatings_df], axis=1)


#duplicate check
duplicates = currentRatings_df[currentRatings_df.duplicated(subset=['locationId'], keep=False)]


# Convert the data to a DataFrame
KeyQs = pd.json_normalize(currentRatings_df['overall.keyQuestionRatings'])


# Create a new DataFrame with the desired columns
new_df = pd.DataFrame({
    'locationId': currentRatings_df['locationId'],
    'name': [item.get('name') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 0]],
    'rating': [item.get('rating') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 0]],
    'reportDate': [item.get('reportDate') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 0]],
    'reportLinkId': [item.get('reportLinkId') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 0]]
})

new_df2 = pd.DataFrame({
    'locationId': currentRatings_df['locationId'],
    'name': [item.get('name') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
    'rating': [item.get('rating') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
    'reportDate': [item.get('reportDate') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
    'reportLinkId': [item.get('reportLinkId') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]]
})

new_df3 = pd.DataFrame({
    'locationId': currentRatings_df['locationId'],
    'name': [item.get('name') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
    'rating': [item.get('rating') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
    'reportDate': [item.get('reportDate') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
    'reportLinkId': [item.get('reportLinkId') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]]
})



new_df4 = pd.DataFrame({
    'locationId': currentRatings_df['locationId'],
    'name': [item.get('name') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
    'rating': [item.get('rating') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
    'reportDate': [item.get('reportDate') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
    'reportLinkId': [item.get('reportLinkId') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]]
})

new_df5 = pd.DataFrame({
    'locationId': currentRatings_df['locationId'],
    'name': [item.get('name') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
    'rating': [item.get('rating') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
    'reportDate': [item.get('reportDate') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
    'reportLinkId': [item.get('reportLinkId') if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]]
})





## Create a new DataFrame with the desired columns
#new_df2 = pd.DataFrame({'locationId': currentRatings_df['locationId'],
#                       'name': [item.get['name'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
#                       'rating': [item.get['rating'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
#                       'reportDate': [item.get['reportDate'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]],
#                       'reportLinkId': [item.get['reportLinkId'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 1]]})
#
## Create a new DataFrame with the desired columns
#new_df3 = pd.DataFrame({'locationId': currentRatings_df['locationId'],
#                       'name': [item['name'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
#                       'rating': [item['rating'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
#                       'reportDate': [item['reportDate'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]],
#                       'reportLinkId': [item['reportLinkId'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 2]]})
#
## Create a new DataFrame with the desired columns
#new_df4 = pd.DataFrame({'locationId': currentRatings_df['locationId'],
#                       'name': [item['name'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
#                       'rating': [item['rating'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
#                       'reportDate': [item['reportDate'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]],
#                       'reportLinkId': [item['reportLinkId'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 3]]})
#
## Create a new DataFrame with the desired columns
#new_df5 = pd.DataFrame({'locationId': currentRatings_df['locationId'],
#                       'name': [item['name'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
#                       'rating': [item['rating'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
#                       'reportDate': [item['reportDate'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]],
#                       'reportLinkId': [item['reportLinkId'] if isinstance(item, dict) else None for item in KeyQs.iloc[:, 4]]})
#
#

new_df = pd.concat([new_df, new_df2, new_df3, new_df4, new_df5])

#save all carehomes
#csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/all_ratings_raw_cr.csv'

# Use the to_csv method to write the DataFrame to a CSV file
#new_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column



#some duplicates exist for locations with current rating before 2014 so the 'effective' 'safe' etc all get 'none' - these get removed
duplicates = new_df[new_df.duplicated(subset=['locationId', 'name'], keep=False)]

new_df = new_df[['locationId', 'name', 'rating']]

pivoted_df = pd.pivot_table(new_df, values='rating', index='locationId', columns='name', aggfunc='first')
# Reset the index
pivoted_df.reset_index(inplace=True)

# Rename the index name to None
pivoted_df.index.name = None

currentRatings_df = currentRatings_df[['locationId', 'reportDate', 'overall.reportLinkId', 'overall.organisationId', 'serviceRatings', 'overall.rating' ]]

currentRatings_df = currentRatings_df.merge(pivoted_df, left_on='locationId', right_on='locationId')
new_df = new_df.merge(currentRatings_df, left_on='locationId', right_on='locationId')

currentRatings_df =  currentRatings_df.rename(columns={"overall.reportLinkId":"reportLinkId","overall.organisationId": "organisationId", "overall.rating": "Overall"})


#save all carehomes
#csv_file_path = 'C:/Users/benjamin.goodair/OneDrive - Nexus365/Documents/current_ratings_clean.csv'

# Use the to_csv method to write the DataFrame to a CSV file
#currentRatings_df.to_csv(csv_file_path, index=False)  # Set index=False to avoid writing row numbers as a column





