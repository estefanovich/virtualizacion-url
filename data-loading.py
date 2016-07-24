import csv
import http.client
import json
import random
import sys
import threading
import queue

csv.field_size_limit(sys.maxsize)

DOMAIN = "z7jvpnl323.execute-api.us-west-2.amazonaws.com"
PATH = "/produ"

requests_queue = queue.Queue(maxsize=32*1024)

def make_request(host, path, method, request_payload):
    connection = http.client.HTTPSConnection(host)
##    print(str(json.dumps(request_payload)))
##    print(path)
    connection.request(method, path, json.dumps(request_payload), {"Content-type": "application/json"})
    response = connection.getresponse()
##    print(str(response.status))
    if (response.status == 200):
        data = response.read().decode("utf-8")
        response_payload = json.loads(data)
        print(str(response_payload))
        if "id" in response_payload.keys():
            return {"status": response.status, "id": response_payload["id"]}
        else:
            return {"status": 500}
    else:
        return {"status": response.status}

def process_request(queue):
    while True:
        request = queue.get()
        result = make_request(request["host"], request["path"], request["method"], request["data"])
        if request["expected_status"] == 200:
            if result["status"] == 200:
                request["target"]["api-id"] = result["id"]
            else:
                print("Error while calling endopoint \"" + request["path"] + "\". Retrying.")
                result = make_request(request["host"], request["path"], request["method"], request["data"])
                if result["status"] == 200:
                    request["target"]["api-id"] = result["id"]
                else:
                    print("Error while calling endopoint \"" + request["path"] + "\". Exiting.")
                    sys.exit()
        else:
            if result["status"] != request["expected_status"]:
                print("Error while calling endopoint \"" + request["path"] + "\". Retrying.")
                result = make_request(request["host"], request["path"], request["method"], request["data"])
                if result["status"] != request["expected_status"]:
                    print("Error while calling endopoint \"" + request["path"] + "\". Exiting.")
        queue.task_done()

for i in range(2):
    thread = threading.Thread(target=process_request, args=(requests_queue,))
    thread.daemon = True
    thread.start()

advertisers = {}
publishers = {}

##********************************************************************************************************************************##
with open("advertisers.csv") as advertisers_file:
    reader = csv.reader(advertisers_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        advertiser = {}
        advertiser["id"] = row[0]
        advertiser["name"] = row[1]
        advertiser["campaigns"] = {}
        advertiser["ads"] = {}
        advertiser["exclusions"] = []
        advertisers[row[0]] = advertiser
advertisers_file.close()

with open("campaigns.csv") as campaigns_file:
    reader = csv.reader(campaigns_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        campaign = {}
        campaign["id"] = row[0]
        campaign["name"] = row[2]
        campaign["category"] = int(row[3])
        campaign["bid"] = int(row[4])
        campaign["budget"] = int(row[5])
        campaign["status"] = row[6] == "True"
        advertisers[row[1]]["campaigns"][row[0]] = campaign
campaigns_file.close()

with open("ads.csv") as ads_file:
    reader = csv.reader(ads_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        ad = {}
        ad["id"] = row[0]
        ad["headline"] = row[2]
        ad["description"] = row[3]
        ad["url"] = row[4]
        advertisers[row[1]]["ads"][row[0]] = ad
ads_file.close()

with open("targetings.csv") as targetings_file:
    reader = csv.reader(targetings_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        advertisers[row[0]]["campaigns"][row[1]]["targeting_zip_codes"] = row[2].split("|")
targetings_file.close()

for advertiser_id, advertiser in advertisers.items():
    for campaign_id, campaign in advertiser["campaigns"].items():
        ads_ids = advertiser["ads"].keys()
        ads_count = random.randint(1, len(ads_ids))
        campaign["ads"] = random.sample(ads_ids, ads_count)

with open("publishers.csv") as publishers_file:
    reader = csv.reader(publishers_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        publisher = {}
        publisher["id"] = row[0]
        publisher["name"] = row[1]
        publisher["campaigns"] = {}
        publishers[row[0]] = publisher
publishers_file.close()

with open("publisher-campaigns.csv") as campaigns_file:
    reader = csv.reader(campaigns_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        campaign = {}
        campaign["id"] = row[0]
        campaign["name"] = row[2]
        campaign["commission"] = row[3]
        publishers[row[1]]["campaigns"][row[0]] = campaign
campaigns_file.close()

publishers_ids = publishers.keys()
for advertiser_id in advertisers.keys():
    _random = random.random()
    if _random < 0.5:
        continue
    elif _random < 0.8:
        exclusions_count = min(random.randint(1, 10), len(publishers_ids) - 1)
    else:
        if len(publishers_ids) <= 10:
            exclusions_count = min(random.randint(1, 10), len(publishers_ids) - 1)
        else:
            exclusions_count = min(random.randint(10, 100), len(publishers_ids)- 1)
    advertisers[advertiser_id]["exclusions"] = random.sample(publishers_ids, exclusions_count)

##print(json.dumps(advertisers, indent=4))

##********************************************************************************************************************************##
for advertiser in advertisers.values():
    print("Creating advertiser \"" + advertiser["name"] + "\".")
    data = {}
    data["name"] = advertiser["name"]

    request = {}
    request["host"] = DOMAIN
    request["path"] = PATH + "/advertisers"
    request["method"] = "POST"
    request["data"] = data
    request["expected_status"] = 200
    request["target"] = advertiser
    requests_queue.put(request)
requests_queue.join()

for advertiser in advertisers.values():
    for campaign in advertiser["campaigns"].values():
        print("Creating advertiser campaign \"" + campaign["name"] + "\".")
        data = {}
        data["name"] = campaign["name"]
        data["category"] = campaign["category"]

        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns"
        request["method"] = "POST"
        request["data"] = data
        request["expected_status"] = 200
        request["target"] = campaign
        requests_queue.put(request)
requests_queue.join()

for advertiser in advertisers.values():
    for campaign in advertiser["campaigns"].values():
        if campaign["status"] == True:
            print("Activating campaign \"" + campaign["name"] + "\".")
            data = {}
            data["status"] = campaign["status"]

            request = {}
            request["host"] = DOMAIN
            request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"])
            request["method"] = "PUT"
            request["data"] = data
            request["expected_status"] = 204
            requests_queue.put(request)
            
        print("Setting $" + str(campaign["bid"]) + " bid to campaign " + campaign["name"] + "\".")
        data = {}
        data["bid"] = campaign["bid"]

        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/bid"
        request["method"] = "PUT"
        request["data"] = data
        request["expected_status"] = 204
        requests_queue.put(request)
        
        if "targeting_zip_codes" in campaign.keys():
            print("Setting " + str(len(campaign["targeting_zip_codes"])) + " zip codes in targeting for campaign " + campaign["name"] + "\".")
            data = {}
            data["zip-codes"] = campaign["targeting_zip_codes"]

            request = {}
            request["host"] = DOMAIN
            request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/targeting"
            request["method"] = "PUT"
            request["data"] = data
            request["expected_status"] = 204
            requests_queue.put(request)

        if campaign["budget"] > 0:
            print("Setting budget of $" + str(campaign["budget"]) + " to campaign " + campaign["name"] + "\".")
            data = {}
            data["budget"] = campaign["budget"]

            request = {}
            request["host"] = DOMAIN
            request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/budget"
            request["method"] = "PUT"
            request["data"] = data
            request["expected_status"] = 204
            requests_queue.put(request)
requests_queue.join()

for advertiser in advertisers.values():
    for ad in advertiser["ads"].values():
        print("Creating ad with headline \"" + ad["headline"] + "\".")
        data = {}
        data["headline"] = ad["headline"]
        data["description"] = ad["description"]
        data["url"] = ad["url"]

        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/ads"
        request["method"] = "POST"
        request["data"] = data
        request["expected_status"] = 200
        request["target"] = ad
        requests_queue.put(request)
requests_queue.join()

for advertiser in advertisers.values():
    for campaign in advertiser["campaigns"].values():
        print("Assigning ads to campaign \"" + campaign["name"] + "\".\n")
        ads = []
        for ad in campaign["ads"]:
            ads.append(advertiser["ads"][ad]["api-id"])
        data = {}
        data["ads"] = ads

        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/ads"
        request["method"] = "PUT"
        request["data"] = data
        request["expected_status"] = 204
        requests_queue.put(request)
requests_queue.join()

##********************************************************************************************************************************##
for publisher in publishers.values():
    print("Creating publisher \"" + publisher["name"] + "\".")
    data = {}
    data["name"] = publisher["name"]

    request = {}
    request["host"] = DOMAIN
    request["path"] = PATH + "/publishers"
    request["method"] = "POST"
    request["data"] = data
    request["expected_status"] = 200
    request["target"] = publisher
    requests_queue.put(request)
requests_queue.join()

for publisher in publishers.values():
    for campaign in publisher["campaigns"].values():
        print("Creating publisher campaign \"" + campaign["name"] + "\".")
        data = {}
        data["name"] = campaign["name"]

        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/publishers/" + str(publisher["api-id"]) + "/campaigns"
        request["method"] = "POST"
        request["data"] = data
        request["expected_status"] = 200
        request["target"] = campaign
        requests_queue.put(request)
requests_queue.join()

for publisher in publishers.values():
    for campaign in publisher["campaigns"].values():
        print("Setting campaign commission to " + str(campaign["commission"]) + "%.\n")
        data = {}
        data["commission"] = campaign["commission"]
        
        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/publishers/" + str(publisher["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/commission"
        request["method"] = "PUT"
        request["data"] = data
        request["expected_status"] = 204
        requests_queue.put(request)
requests_queue.join()

for advertiser in advertisers.values():
    exclusions = []
    for exclusion in advertiser["exclusions"]:
        exclusions.append(publishers[exclusion]["api-id"])
    if len(exclusions) > 0:
        print("Setting advertiser \"" + advertiser["name"] + "\" exclusions. " + str(len(exclusions)) + " publishers excluded.\n")
        data = {}
        data["publishers"] = exclusions
        
        request = {}
        request["host"] = DOMAIN
        request["path"] = PATH + "/advertisers/" + str(advertiser["api-id"]) + "/exclusions"
        request["method"] = "PUT"
        request["data"] = data
        request["expected_status"] = 204
        requests_queue.put(request)
requests_queue.join()
