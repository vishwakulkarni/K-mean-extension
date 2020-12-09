import json
import csv
from datetime import datetime
import requests
post_request_headers = {
"Content-Type": "application/json"
}
csv_path = 'traffic_accidents.csv'
json_path = 'parsed _data.json'
data = {}
dist_map = {
	0: "Others",
    1:"City and County of Denver",
    2:"northwestern suburbs of Denver",
    3:"rural Western Slope",
    4:"rural Eastern Plains",
    5:"Colorado Springs",
    6:"Denver-Aurora Metropolitan Area",
    7:"western parts of the Denver-Aurora Metropolitan Area"
}
es_url = 'http://127.0.0.1:9200'
es_index = 'electric'
with open(csv_path) as csvdata:
    csvReader = csv.DictReader(csvdata)
    for row in csvReader:
        id = int(row['OBJECTID_1'])
        row['OBJECTID_1']=int(row['OBJECTID_1'])
        row['INCIDENT_ID']=int(float(row['INCIDENT_ID']))
        row['OFFENSE_ID']=int(row['OFFENSE_ID'])
        row['OFFENSE_CODE']=int(row['OFFENSE_CODE'])
        del row['OFFENSE_CODE_EXTENSION']
        #row['TOP_TRAFFIC_ACCIDENT_OFFENSE']=row['TOP_TRAFFIC_ACCIDENT_OFFENSE'].replace(' ','_')
        
        del row['LAST_OCCURRENCE_DATE']
        try:
            datetime.strptime(row['FIRST_OCCURRENCE_DATE'],"%Y-%m-%d %H:%M:%S")
            datetime.strptime(row['REPORTED_DATE'],"%Y-%m-%d %H:%M:%S")
            row['GEO_X']=int(row['GEO_X'])
            row['GEO_Y']=int(row['GEO_Y'])

        except ValueError:
            continue
        row['INCIDENT_ADDRESS']=row['INCIDENT_ADDRESS'].replace(' ','_')
        
        
        row['GEO_LON']=float(row['GEO_LON'])
        row['GEO_LAT']=float(row['GEO_LAT'])
        row['location'] = [row['GEO_LON'],row['GEO_LAT']]
        if(row['DISTRICT_ID']!=''):
            row['DISTRICT_ID']=int(row['DISTRICT_ID'])
            
        else:
            row['DISTRICT_ID']=0

        row['district'] = dist_map[row['DISTRICT_ID']]

        if(row['PRECINCT_ID']!=''):
            
            row['PRECINCT_ID']=int(row['PRECINCT_ID'])
        else:
            
            row['PRECINCT_ID'] = 0
        if (row['FATALITIES'] != ''):

            row['FATALITIES'] = float(row['FATALITIES'])
        else:

            row['FATALITIES'] = 0
        if (row['SERIOUSLY_INJURED'] != ''):

            row['SERIOUSLY_INJURED'] = float(row['SERIOUSLY_INJURED'])
        else:

            row['SERIOUSLY_INJURED'] = 0

        
        del row['BICYCLE_IND']
        del row['PEDESTRIAN_IND']
        data[id] = row
        # print((row))
        insert_request = requests.post(url="{}/{}/_doc/{}".format(es_url, es_index, row['OBJECTID_1']),
                                       data=json.dumps(row),
                                       headers=post_request_headers).json()
        # print(insert_request)
        # print(row)

with open(json_path,'w')as jsonFile:
    jsonFile.write(json.dumps(data,indent=4))


