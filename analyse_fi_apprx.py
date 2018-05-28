import csv
import glob
import json
import os


def file_is_empty(path):
    return os.stat(path).st_size == 0


def process_file_fi(path, name):
    fieldnames = ["experimentNum", "runtime", "thread", "emitNum", "estimate", "item", "lowerBound", "upperBound",
                  "mapSize"]
    with open(name, 'w', newline='') as csv_write_file:
        print("Opened file")
        with open(path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(csv_write_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                print(reader.line_num - 1)
                print(row['resultPath'])
                result_files = glob.glob(row['resultPath'] + '/*')
                for thread, file in enumerate(result_files, 1):
                    if not file_is_empty(file):
                        print("process file" + file)
                        with open(file) as result_file:
                            for line_number, line in enumerate(result_file, 1):
                                if len(line.strip()) != 0:
                                    parsed_line = json.loads(line)
                                    if 'resultList' in parsed_line:
                                        for i, result_item in enumerate(parsed_line['resultList']):
                                            writer.writerow(
                                                {'runtime': row['runtime'], 'experimentNum': reader.line_num - 1,
                                                 "thread": thread, 'emitNum': line_number, "item": result_item['item'],
                                                 "estimate": result_item['estimate'],
                                                 "lowerBound": result_item['lowerBound'],
                                                 "upperBound": result_item['upperBound'], "mapSize": row['mapSize']})


def process_file_fi_exact(path, name):
    fieldnames = ["experimentNum", "runtime", "thread", "emitNum", "estimate", "item"]
    with open(name, 'w', newline='') as csv_write_file:
        print("Opened file")
        with open(path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(csv_write_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                print(reader.line_num - 1)
                print(row['resultPath'])
                result_files = glob.glob(row['resultPath'] + '/*')
                for thread, file in enumerate(result_files, 1):
                    if not file_is_empty(file):
                        print("process file" + file)
                        with open(file) as result_file:
                            for line_number, line in enumerate(result_file, 1):
                                if len(line.strip()) != 0:
                                    parsed_line = json.loads(line)
                                    if 'frequentItems' in parsed_line:
                                        for i, result_item in enumerate(parsed_line['frequentItems']):
                                            writer.writerow(
                                                {'runtime': row['runtime'], 'experimentNum': reader.line_num - 1,
                                                 "thread": thread, 'emitNum': line_number, "item": result_item,
                                                 "estimate": parsed_line['frequentItems'][result_item]})

def process_file_hll(path, name):
    fieldnames = ["experimentNum", "runtime", "thread", "emitNum", "estimate", "item"]
    with open(name, 'w', newline='') as csv_write_file:
        print("Opened file")
        with open(path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(csv_write_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                print(reader.line_num - 1)
                print(row['resultPath'])
                result_files = glob.glob(row['resultPath'] + '/*')
                for thread, file in enumerate(result_files, 1):
                    if not file_is_empty(file):
                        print("process file" + file)
                        with open(file) as result_file:
                            for line_number, line in enumerate(result_file, 1):
                                if len(line.strip()) != 0:
                                    parsed_line = json.loads(line)
                                    if 'resultList' in parsed_line:
                                        for i, result_item in enumerate(parsed_line['resultList']):
                                            writer.writerow(
                                                {'runtime': row['runtime'], 'experimentNum': reader.line_num - 1,
                                                 "thread": thread, 'emitNum': line_number, "item": result_item['key'],
                                                 "estimate": result_item['estimate']})


#process_file_fi('AZFI_APPRX.csv', 'AZFI_APPRX_Rating_Processed.csv')
#process_file_fi('ILFI_APPRX.csv','ILFI_APPRX_Processed.csv')
#process_file_fi_exact('AZFI_EXACT.csv', 'AZFI_EXACT_Rating_Processed.csv')
process_file_hll('AZHLL_APPRX.csv','AZHLL_APPRX_REVIEWER_PRODUCT_processed.csv')