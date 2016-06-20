import random
import csv

campaigns_file = open("campaigns.csv", "w")
campaigns_file.write("Campaign ID,Advertiser ID,Campaign name,Category,Bid,Budget,Status\n")
campaigns_count = 0
with open("advertisers.csv") as advertisers_file:
    reader = csv.reader(advertisers_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        total_campaigns = random.randint(1,3)
        for i in range (1, total_campaigns + 1):
            campaigns_count = campaigns_count + 1
            category = random.randint(1, 3)
            bid = random.randint(1, 10)
            budget = random.randint(0, 10) * 100
            if random.random() < 0.33:
                status = False
            else:
                status = True
            campaigns_file.write(str(campaigns_count) + "," + row[0] + ",Advertiser XYZ " + row[0] + " - Campaign ABC " + str(i) + "," + str(category) + "," + str(bid) + "," + str(budget) + "," + str(status) + "\n")
advertisers_file.close()
campaigns_file.close()
