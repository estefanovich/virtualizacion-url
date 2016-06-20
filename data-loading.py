import csv
import http.client
import json
import random
import sys

DOMAIN = "dtlh97kqh2.execute-api.us-east-1.amazonaws.com"
PATH = "/prod"

def make_request(domain, path, method, request_payload):
    connection = http.client.HTTPSConnection(domain)
##    print(str(json.dumps(request_payload)))
##    print(path)
    connection.request(method, path, json.dumps(request_payload), {"Content-type": "application/json"})
    response = connection.getresponse()
##    print(str(response.status))
    if (response.status == 200):
        data = response.read().decode("utf-8")
        response_payload = json.loads(data)
        if "id" in response_payload.keys():
            object_id = response_payload["id"]
        else:
            object_id = None
        return {"status": response.status, "id": object_id}
    else:
        return {"status": response.status}

advertisers = {}
publishers = {}

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

for advertiser in advertisers.values():
    print("Creating advertiser \"" + advertiser["name"] + "\".")
    data = {}
    data["name"] = advertiser["name"]
    result = make_request(DOMAIN, PATH + "/advertisers", "POST", data)
    if result["status"] == 200:
        print("Advertiser ID: " + str(result["id"]) + ".\n")
        advertiser["api-id"] = result["id"]
    else:
        print("Error creating advertiser: Invalid response.")
        sys.exit()

    for campaign in advertiser["campaigns"].values():
        print("Creating campaign \"" + campaign["name"] + "\".")
        data = {}
        data["name"] = campaign["name"]
        data["category"] = campaign["category"]
        result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns", "POST", data)
        if result["status"] == 200:
            print("Campaign ID: " + str(result["id"]) + ".\n")
            campaign["api-id"] = result["id"]
        else:
            print("Error creating campaign: Invalid response.")
            sys.exit()
        
        if campaign["status"] == True:
            print("Activating campaign.\n")
            data = {}
            data["status"] = campaign["status"]
            result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]), "PUT", data)
            if result["status"] != 204:
                print("Error activating campaign: Invalid response.")
                sys.exit()

        print("Setting campaign bid to $" + str(campaign["bid"]) + ".\n")
        data = {}
        data["bid"] = campaign["bid"]
        result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/bid", "PUT", data)
        if result["status"] != 204:
            print("Error setting campaign bid: Invalid response.")
            sys.exit()

        if "targeting_zip_codes" in campaign.keys():
            print("Setting campaign targeting. " + str(len(campaign["targeting_zip_codes"])) + " zip codes in targeting.\n")
            data = {}
            data["zip-codes"] = campaign["targeting_zip_codes"]
            result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/targeting", "PUT", data)
            if result["status"] != 204:
                print("Error setting campaign targeting: Invalid response.")
                sys.exit()


        if campaign["budget"] > 0:
            data = {}
            data["budget"] = campaign["budget"]
            print("Setting campaign budget to $" + str(campaign["budget"]) + ".\n")
            result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/budget", "PUT", data)
            if result["status"] != 204:
                print("Error setting campaign bid: Invalid response.")
                sys.exit()

    for ad in advertiser["ads"].values():
        data = {}
        data["headline"] = ad["headline"]
        data["description"] = ad["description"]
        data["url"] = ad["url"]
        print("Creating ad with headline \"" + ad["headline"] + "\".")
        result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/ads", "POST", data)
        if result["status"] == 200:
            print("Ad ID: " + str(result["id"]) + ".\n")
            ad["api-id"] = result["id"]
        else:
            print("Error creating campaign: Invalid response.")
            sys.exit()

    for campaign in advertiser["campaigns"].values():
        ads = []
        for ad in campaign["ads"]:
            ads.append(advertiser["ads"][ad]["api-id"])
        data = {}
        data["ads"] = ads
        result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/ads", "PUT", data)
        print("Assigning ads to campaign \"" + campaign["name"] + "\".\n")
        if result["status"] != 204:
            print("Error assigning ads to campaign: Invalid response.")
            sys.exit()

for publisher in publishers.values():
    print("Creating publisher \"" + publisher["name"] + "\".")
    data = {}
    data["name"] = publisher["name"]
    result = make_request(DOMAIN, PATH + "/publishers", "POST", data)
    if result["status"] == 200:
        print("Publisher ID: " + str(result["id"]) + ".\n")
        publisher["api-id"] = result["id"]
    else:
        print("Error creating publisher: Invalid response.")
        sys.exit()

    for campaign in publisher["campaigns"].values():
        data = {}
        data["name"] = campaign["name"]
        result = make_request(DOMAIN, PATH + "/publishers/" + str(publisher["api-id"]) + "/campaigns", "POST", data)
        if result["status"] == 200:
            print("Campaign ID: " + str(result["id"]) + ".\n")
            campaign["api-id"] = result["id"]
        else:
            print("Error creating campaign: Invalid response.")
            sys.exit()

        print("Setting campaign commission to " + str(campaign["commission"]) + "%.\n")
        data = {}
##        data["commission"] = campaign["commission"]
        data["comission"] = campaign["commission"]
##        result = make_request(DOMAIN, PATH + "/publishers/" + str(publisher["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/commission", "PUT", data)
        result = make_request(DOMAIN, PATH + "/publishers/" + str(publisher["api-id"]) + "/campaigns/" + str(campaign["api-id"]) + "/comission", "PUT", data)
        if result["status"] != 204:
            print("Error setting campaign commission: Invalid response.")
            sys.exit()

for advertiser in advertisers.values():
    exclusions = []
    for exclusion in advertiser["exclusions"]:
        exclusions.append(publishers[exclusion]["api-id"])
    if len(exclusions) > 0:
        print("Setting advertiser \"" + advertiser["name"] + "\" exclusions. " + str(len(exclusions)) + " publishers excluded.\n")
        data = {}
        data["publishers"] = exclusions
        result = make_request(DOMAIN, PATH + "/advertisers/" + str(advertiser["api-id"]) + "/exclusions", "PUT", data)
        if result["status"] != 204:
            print("Error setting advertiser exclusions: Invalid response.")
            sys.exit()


