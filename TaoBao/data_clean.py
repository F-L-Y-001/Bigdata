import csv

with open('UserBehavior.csv', 'r') as infile, \
     open('Processed_UserBehavior.csv', 'w', newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    for row in reader:
        try:
            datetime_str = row[3]
            date_part = datetime_str.split()[0]

            if date_part != '2017-11-25':
                writer.writerow(row)
        except (IndexError, ValueError):
            continue

print("已成功清洗掉 2017-11-25 的数据，结果保存至 Cleaned_UserBehavior.csv")