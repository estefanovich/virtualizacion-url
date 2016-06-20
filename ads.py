import random
import csv

##ads_file = open("ads.csv", "w")
##ads_file.write("Ad ID,Advertiser ID,Campaign ID,Headline,Description,URL\n")
##ads_count = 0
##with open("campaigns.csv") as campaigns_file:
##    reader = csv.reader(campaigns_file)
##    for row in reader:
##        if reader.line_num == 1:
##            continue
##        ads_count = ads_count + 1
##        _random = random.random()
##        if (_random < 0.2):
##            total_ads = 3
##        elif (_random < 0.5):
##            total_ads = 2
##        else:
##            total_ads = 1
##        for i in range(1, total_ads + 1):
##            ads_file.write(str(ads_count) + "," + row[1] + "," + row[1] + "Buy our products"\n")
##ads_file.close()
##campaigns_file.close()

ads_file = open("ads.csv", "w")
ads_file.write("Ad ID,Advertiser ID,Headline,Description,URL\n")
ads_count = 0
with open("advertisers.csv") as advertisers_file:
    reader = csv.reader(advertisers_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        for j in range(1, int(row[2]) + 1):
            ads_count = ads_count + 1
            ads_file.write(str(ads_count) + "," + row[0] + ",Ad from advertiser #" + row[0] + ",Ad #" + str(j) + " of advertiser #" + row[0] + ",https://www.amazon.com?advertiser=" + row[0] + "&ad=" + str(j) + "\n") 
ads_file.close()
advertisers_file.close()
