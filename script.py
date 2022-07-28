import json
import pandas as pd

with open('data_1.json') as data_1:
    read_content_1 = json.load(data_1)
with open('data_2.json') as data_2:
    read_content_2 = json.load(data_2)
with open('data_3.json') as data_3:
    read_content_3 = json.load(data_3)

# Each contains single top level dictionary
read_content_1 = read_content_1[0]
read_content_2 = read_content_2[0]
read_content_3 = read_content_3[0]

# Create empty dictionary that will hold the constant values from the top and 
# mid levels of the hiearchy
constants_dict_1 = {}
constants_dict_2 = {}
constants_dict_3 = {}

# Retrieve the top level constants, 3 different dictionaries here 
# just in case the values differ between json 1,2,3
top_const_keys = ['tenantName', 'eventType', 'eventTimeEpoch']

for key in top_const_keys:
    constants_dict_1.update({key : read_content_1.get(key)})
    constants_dict_2.update({key : read_content_2.get(key)})
    constants_dict_3.update({key : read_content_3.get(key)})    

# These will hold the individual readings and will have the constant
# data injected into them later
data_1 = read_content_1['payload'].pop('data')
data_2 = read_content_2['payload'].pop('data')
data_3 = read_content_3['payload'].pop('data')

# Add the mid level constant values to the top level constant values
# Read_content['payload'] leftover after the pop represents the mid level
# constants in the json data. 
mid_lvl_constants_1 = read_content_1['payload']
constants_dict_1.update(mid_lvl_constants_1)

mid_lvl_constants_2 = read_content_2['payload']
constants_dict_2.update(mid_lvl_constants_2)

mid_lvl_constants_3 = read_content_3['payload']
constants_dict_3.update(mid_lvl_constants_3)

# Add the constant values to the individual readings
for reading in data_1:
    reading.update(constants_dict_1)

for reading in data_2:
    reading.update(constants_dict_2)

for reading in data_3:
    reading.update(constants_dict_3)

# Aggregate readings from all three json files
data_total = data_1 + data_2 + data_3

#Create DataFrame and reorder columns, export
df = pd.DataFrame(data_total)
df = df[['tenantName', 'eventType', 'eventTimeEpoch', 'antenna',
         'datetime', 'readername', 'epc', 'deviceid', 'user', 
         'tagevent', 'peakrssi', 'accountuuid', 'version']]

df.to_csv('final_table.csv', index=None)