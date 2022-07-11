import time as time
import pandas as pd
import os, uuid, sys
from datetime import datetime, timedelta
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake._models import ContentSettings
from random import randint

# # Storage Account Key
storage_account_key = 'OwHPjp/u8EXbX7Mdh7Z9nP6fexnWxxVDsPKvH0cimhs2cP+3TwE1ahbmI5IcWYuzvt1xrhKhtDGo+ASt5zLwkg=='
storage_account_name = 'bigdatatableaustorageacc'

# # Open the other half of the csv and create a list
data = pd.read_csv('query.csv')

# # Sorting value
data = data.sort_values('time')

csv_time = data['time'].tolist()
latitude = data['latitude'].tolist()
longitude = data['longitude'].tolist()
depth = data['depth'].tolist()
mag = data['mag'].tolist()
magType = data['magType'].tolist()
nst = data['nst'].tolist()
gap = data['gap'].tolist()
dmin = data['dmin'].tolist()
rms = data['rms'].tolist()
net = data['net'].tolist()
ids = data['id'].tolist()
updated = data['updated'].tolist()
place = data['place'].tolist()
types = data['type'].tolist()
horizontalError = data['horizontalError'].tolist()
depthError = data['depthError'].tolist()
magError = data['magError'].tolist()
magNst = data['magNst'].tolist()
status = data['status'].tolist()
locationSource = data['locationSource'].tolist()
magSource = data['magSource'].tolist()


# date = str(datetime.now().date())
# time = str(datetime.now().time())[:-3]
# date_format_needed = date + 'T' + time + 'Z'

# # Authenticate the Storage
def initialize_storage_account(storage_account_name, storage_account_key):
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        # print('Login Success')
    except Exception as e:
        print('Login Failure')
        # print(e)

# # Open the csv file and add data
def edit_csv_file(
        date_to_feed,
        latitude,
        longitude,
        depth,
        mag,
        magType,
        nst,
        gap,
        dmin,
        rms,
        net,
        id,
        updated,
        place,
        types,
        horizontalError,
        depthError,
        magError,
        magNst,
        status,
        locationSource,
        magSource
        ):
    data = pd.read_csv('newfile.csv')
    # print(data)
    data = data.append({
        'time': date_to_feed,
        'latitude': latitude,
        'longitude':longitude,
        'depth':depth,
        'mag':mag,
        'magType': magType,
        'nst':nst,
        'gap':gap,
        'dmin':dmin,
        'rms':rms,
        'net':net,
        'id': id,
        'updated':updated,
        'place':place,
        'type': types,
        'horizontalError':horizontalError,
        'depthError':depthError,
        'magError':magError,
        'magNst':magNst,
        'status':status,
        'locationSource':locationSource,
        'magSource':magSource
        }, ignore_index=True)
    # print(data)
    data.to_csv('newfile.csv', index=False)


# # Upload a file to a directory
def upload_file_to_directory():
    print('uploadfiletodirectoruy')
    try:
        print('File uploaded')
        file_system_client = service_client.get_file_system_client(file_system="data")
        # directory_client = file_system_client.get_directory_client("test1")
        file_client = file_system_client.create_file("finaldisaster.csv")
        local_file = open("newfile.csv",'r', encoding="utf8")
        file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
    except Exception as e:
      print(e)


initialize_storage_account(storage_account_name, storage_account_key)
given_date = datetime(2022, 6, 24, 18, 30, 0)
counter = 0
while True:
    
    given_date += timedelta(seconds=1)
    date_to_feed = str(given_date)[:10] + 'T' + str(given_date)[11:] + '.000Z'
    counter = counter + 1
    # random_number = randint(1, len(magSource))
    random_number = counter
    edit_csv_file(
        # date_to_feed,
        csv_time[random_number],
        latitude[random_number],
        longitude[random_number],
        depth[random_number],
        mag[random_number],
        magType[random_number],
        nst[random_number],
        gap[random_number],
        dmin[random_number],
        rms[random_number],
        net[random_number],
        counter,
        updated[random_number],
        place[random_number],
        types[random_number],
        horizontalError[random_number],
        depthError[random_number],
        magError[random_number],
        magNst[random_number],
        status[random_number],
        locationSource[random_number],
        magSource[random_number]
        )
    upload_file_to_directory()
    time.sleep(5)









