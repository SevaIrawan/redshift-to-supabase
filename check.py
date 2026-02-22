import csv

count = 0
with open("blue_whale_sgd_export.csv", newline="", encoding="utf-8") as f:
    for _ in csv.reader(f):
        count += 1

print("Total rows (including header):", count)
print("Data rows:", count - 1)
