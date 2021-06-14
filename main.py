import json
import sys
from distutils.log import Log
from random import randrange
import couchdb
import requests
import urllib3
import argparse


BASE_URL = "https://atunwadigital-recast.streamguys1.com"
endpoint = BASE_URL + "/oauth/token"

parser = argparse.ArgumentParser()
parser.add_argument('username', nargs='+')
parser.add_argument('password', nargs='+')
parser.add_argument('client_secret', nargs='+')
parser.add_argument('action', nargs='+')
args = parser.parse_args()
###

# init couchdb
DB_URL = "http://localhost:5984"
DB_USERNAME = "admin"
DB_PSWD = str(args.password[0])
DB_groups = "groups"
DB_groupings = "groupings"
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

####
if DB_groupings in couch:
    DB_groupings = couch[DB_groupings]
else:
    DB_groupings = couch.create(DB_groupings)


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
def doGrouping(group_db, grouping_db):
    allgrouplist = []
    for docid in group_db.view('_all_docs'):
        document = group_db.get(docid.key);
        newChildren = []
        for item in document:
            if "parent" in item:
                # print((document.get(item))) # print parent id
                try:
                    if str(document.get(item)) in group_db:
                        parent_doc = group_db.get(str(document.get(item)))
                        if str(parent_doc.id) in grouping_db:
                            gp_doc = grouping_db.get(parent_doc.id)
                            group_child = json.dumps(gp_doc['children'])
                            oldChild = [];
                            if json.loads(group_child):
                                for child in json.loads(group_child):
                                    for baby in child:
                                        if "_id" == baby:
                                            oldChild.append(child[baby])
                                    newChildren.append(child)
                                if docid.key not in oldChild:
                                    # print(docid.key)
                                    newChildren.append(document)
                            else:
                                newChildren.append(document)
                            gp_doc['children'] = newChildren
                            grouping_db.save(gp_doc)
                        else:
                            if parent_doc["_id"] not in allgrouplist:
                                allgrouplist.append(parent_doc["_id"])
                    else:
                        # Couldnt find parent
                        print("Couldnt find parent")
                        doc = group_db.get(docid.key)
                        rev = doc.pop("_rev")
                        if str(docid.key) not in grouping_db:
                            try:
                                newDoc = grouping_db[docid.key] = doc
                                newDoc['children'] = []
                                grouping_db.save(newDoc)
                                print("Document parent doesnt exist.. saved as parent")
                            except:
                                print(sys.exc_info()[0])
                                pass
                        else:
                            print("Document parent doesnt exist.. saved as parent already")

                    pass

                except:
                    print(sys.exc_info()[0])
                    print("Error couch")
                    pass
    if allgrouplist:
        for parent_id in allgrouplist:
            doc = group_db.get(parent_id)
            rev = doc.pop("_rev")
            if str(parent_id) not in grouping_db:
                try:
                    newDoc = grouping_db[parent_id] = doc
                    newDoc['children'] = []
                    grouping_db.save(newDoc)
                    print("Added children object")
                except:
                    print(sys.exc_info()[0])
                    pass
            else:
                print("Already there")

if str(args.action[0]) == "1":
    getData("/api/v1/groups", DB_groups)
    getData("/api/v1/sgrecast/content", DB_content)
elif str(args.action[0]) == "2":
    doGrouping( DB_groups, DB_groupings)
else :
    print("No action given (1) Backup db, (2) Data Grouping")