import random
import csv

campaigns_file = open("publisher-campaigns.csv", "w")
campaigns_file.write("Campaign ID,Publisher ID,Campaign name,Commission\n")
campaigns_count = 0
with open("publishers.csv") as publishers_file:
    reader = csv.reader(publishers_file)
    for row in reader:
        if reader.line_num == 1:
            continue
        total_campaigns = random.randint(1,4)
        for i in range (1, total_campaigns + 1):
            campaigns_count = campaigns_count + 1
            commission = random.randint(1,10) * 10
            campaigns_file.write(str(campaigns_count) + "," + row[0] + ",Publisher XYZ " + row[0] + " - Campaign ABC " + str(i) + "," + str(commission) + "\n")
publishers_file.close()
campaigns_file.close()
