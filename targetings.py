import random
import csv

zip_codes = []
with open("zip-codes.csv") as zip_codes_file:
    reader = csv.reader(zip_codes_file)
    for row in reader:
        zip_codes.append(row[0])
zip_codes_file.close()

targeting = random.sample(zip_codes, 10)

targetings_file = open("targetings.csv", "w", newline="")
targetings_file.write("Advertiser ID,Campaign ID,Zip codes\n")

with open("campaigns.csv") as campaigns_file:
    reader = csv.reader(campaigns_file)
    writer = csv.writer(targetings_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        if reader.line_num > 15:
            continue        
        _random = random.random()
        if _random < 0.5:
            continue
        _random = random.random()
        if _random < 0.3:
            zip_codes_count = random.randint(10, 100)
        elif _random < 0.6:
            zip_codes_count = random.randint(100, 500)
        elif _random < 0.9:
            zip_codes_count = random.randint(500, 1000)
        else:
            zip_codes_count = random.randint(1000, 40000)
        _targeting = random.sample(zip_codes, zip_codes_count)
##        _targeting = random.sample(zip_codes, 10)
        targeting = str.join("|", _targeting)
        writer.writerow([row[1],row[0],targeting])
campaigns_file.close()
targetings_file.close()
