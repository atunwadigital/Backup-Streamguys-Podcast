import json
from distutils.log import Log

import couchdb
import requests
import urllib3
import argparse

# defining the api-endpoint
BASE_URL = "https://atunwadigital-recast.streamguys1.com"
endpoint = BASE_URL + "/oauth/token"

parser = argparse.ArgumentParser()
parser.add_argument('username', nargs='+')
parser.add_argument('password', nargs='+')
parser.add_argument('client_secret', nargs='+')
args = parser.parse_args()
###

# init couchdb
DB_URL = "http://localhost:5984"
DB_USERNAME = "admin"
DB_PSWD = str(args.password[0])
DB_groups = "groups"
DB_content = "contents"

#####

# creat database if doesnt exist
couch = couchdb.Server(DB_URL)
couch.resource.credentials = (DB_USERNAME, DB_PSWD)
####
if DB_content in couch:
    DB_content = couch[DB_content]
else:
    DB_content = couch.create(DB_content)

####
if DB_groups in couch:
    DB_groups = couch[DB_groups]
else:
    DB_groups = couch.create(DB_groups)

# credentials for connection to streamguys
#
data = {'username': str(args.username[0]),
        'client_secret': str(args.client_secret[0]),
        'client_id': 3,
        'grant_type': "password",
        'password': str(args.password[0]),
        'scope': '*'}

http = urllib3.PoolManager()

# sending post request and saving response as response object
r = requests.post(url=endpoint, data=data)
json_data = json.loads(r.text)
if "error" in json_data:
    print(json_data)
    exit()

token_type = json_data['access_token']
refresh_token = json_data['refresh_token']


# Get content list from streamguys using token
class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


###
def getDataFromPages(url, pageNumber, token_type):
    endpoint = url + "?page=" + str(pageNumber)
    data = requests.get(endpoint, auth=BearerAuth(token_type))
    return json.loads(data.text)


###

##
def getData(apiExtention, couch_db):
    query_pg_number = 1
    endpoint = BASE_URL + apiExtention
    request = requests.get(endpoint, auth=BearerAuth(token_type))
    request_data = json.loads(request.text)
    rq_last_page = request_data["meta"]["last_page"]
    for rq_page in range(0, int(rq_last_page), 1):
        query_pg_number = query_pg_number + 1
        data_returned = getDataFromPages(endpoint, query_pg_number, token_type)
        for Item in data_returned["data"]:
            print(Item)
            try:
                couch_db[str(Item["id"])] = Item
                if "url" in Item :
                    #filedata = urllib3.response(Item["url"])
                    r = http.request('GET', Item["url"])
                    datatowrite = r.data
                    couch_db.put_attachment(couch_db[str(Item["id"])], datatowrite, Item["filename"], content_type=str(Item["mimetype"]))
            except couchdb.http.ResourceConflict:
                pass
        # DB_groups.update(Item)
        # print(Item["id"])


##

getData("/api/v1/groups", DB_groups)

getData("/api/v1/sgrecast/content", DB_content)

