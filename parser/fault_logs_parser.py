# Algo:
# 1. Sort files by date (numeric order)
# 2. Data in logs not sequential, need to parse individual (expand dates) row,
#    open four writers to write to separate files. Store as list and sort (since small data size).
# 3. Get date from filename itself. Starting date would be good.
# ? Need to separate by error types?

# Important assumptions:
# 1. Individual files start from 12:00:00 AM
# 2. Data files must be contiguous, e.g. July -> August
# 3. Filenames of format:
#    "/Fault Logs/Equipment Error Log_<sYYYY>-<sMM>-<sDD>_<eYYYY>-<eMM>-<eDD>.csv"



######################
##  USER VARIABLES  ##
######################

clean_temp_files = True
custom_suffix    = "_fault"
mins_delta       = 60   # 60 == 1 hour interval





#####################
##  MAIN FUNCTION  ##
#####################

import statistics # mean, median
import os, sys
import csv
from datetime import datetime, timedelta

def main():
    print("Initialising directories...")
    clear_temp_dir()
    initialise_dir()
    print("Starting job...")
    # Fill here
    print("Job completed.")
    if clean_temp_files: clear_temp_dir()
    print("Artifacts cleaned.")
    return




#################
##  DIRECTORY  ##
#################

data_path = "data" # Change this to reflect data location
temp_path = "temp"
out_path = "out"
agg_path = str(mins_delta) + custom_suffix

root_dir = os.getcwd() + "\\..\\"
data_dir = root_dir + data_path
temp_dir = root_dir + temp_path
out_dir = root_dir + out_path
agg_dir = root_dir + out_path + "\\" + agg_path

def initialise_dir():
    if data_path not in os.listdir(root_dir): os.mkdir(data_dir)
    if temp_path not in os.listdir(root_dir): os.mkdir(temp_dir)
    if out_path not in os.listdir(root_dir): os.mkdir(out_dir)
    if agg_path not in os.listdir(out_dir): os.mkdir(agg_dir)

def clear_temp_dir():
    if temp_path in os.listdir(root_dir):
        for file in os.listdir(temp_dir):
            os.remove(temp_dir + "\\" + file)
        os.rmdir(temp_dir)
    



##############################
##  INDIVIDUAL FILE PARSER  ##
##############################

pos_err = 1
neg_err = 0

def tokenize_raw_filename(filename):
    default_log, start, end = filename.split("_")
    year, month, day = start.split("-")
    return year, month, day

def parse_file(filename, mins_delta=60, file_dir=os.getcwd()):
    """ mins_delta specifies data duration interval """

    # Retrieve name
    year, month, day = tokenize_raw_filename(filename)
    data_parser = lambda x: parse_row_data(parse_fault(x))

    # Save infile as list (since small dataset)
    header = None
    data = []
    with open(file_dir + "\\" + filename) as infile:
        reader = csv.reader(infile)
        
        # Header
        timestamp_header = ["year","month","day","hour","minute"]
        data_headers = ["error"]
        header = timestamp_header + data_headers
        next(reader)

        # Data
        for row in reader:
            data.append(data_parser(row))

    # Split list into separate error types
    dataset = {}
    for row in data:
        curr_chiller = row[2]
        curr_err = row[3]
        if curr_chiller not in dataset:
            dataset[curr_chiller] = {}
        if curr_err not in dataset[curr_chiller]:
            dataset[curr_chiller][curr_err] = []
        dataset[curr_chiller][curr_err].append((row[0], row[1]))

    # Batch outfiles and writers of error types
    create_filename = lambda x: open(temp_dir + "\\" + x + ".csv", "w", newline="")
    outfiles = {}
    writers = {}
    for chiller in dataset:
        outfiles[chiller] = {}
        writers[chiller] = {}
        for err in dataset[chiller]:
            outfilename = "{} {} {} {}".format(year, month, "Chiller" + chiller[2], err)
            outfiles[chiller][err] = create_filename(outfilename)
            writers[chiller][err] = csv.writer(outfiles[chiller][err])

    # Process header and data
    for chiller in writers:
        for err in writers[chiller]:
            writer = writers[chiller][err]
            data = dataset[chiller][err]
            
            writer.writerow(header) # header row
            data.sort(key=lambda x: x[0]) # ensure running order
            start_dt = datetime(int(year), int(month), 1) # Initialise first day
            end_dt = start_dt.replace(month=start_dt.month+1) if start_dt.month != 12 \
                     else start_dt.replace(year=start_dt.year+1, month=1) # timedelta does not support month increment

            dt = start_dt
            while dt < end_dt:
                if data:
                    left_dt, right_dt = data.pop(0)
                    while dt < left_dt:
                        writer.writerow(unpack_timestamp(dt) + [neg_err])
                        dt += timedelta(minutes=mins_delta)
                    while left_dt <= dt <= right_dt: # assuming right_dt <= end_dt
                        writer.writerow(unpack_timestamp(dt) + [pos_err])
                        dt += timedelta(minutes=mins_delta)
                else:
                    writer.writerow(unpack_timestamp(dt) + [neg_err])
                    dt += timedelta(minutes=mins_delta)

    # Batch close
    for chiller in outfiles:
        for err in outfiles[chiller]:
            outfiles[chiller][err].close()

#####################
##  Parsing Logic  ##
#####################

def unpack_timestamp(dt):
    """ Unpacks datetime object as list up to minute resolution """
    return [dt.year, dt.month, dt.day, dt.hour, dt.minute]

def parse_timestamp(timestamp):
    ### Converts '2017-08-01 00:02:09.937' to datetime object
    year, month, day = map(int, timestamp[:10].split("-"))
    hour, minute, seconds = map(int, timestamp[11:19].split(":"))
    return datetime(year, month, day, hour, minute)

def parse_row_data(data_row):
    """ Parse relevant data columns after initial parse """
    s_ts = parse_timestamp(data_row[0])
    e_ts = parse_timestamp(data_row[1])
    others = data_row[2:]
    return [s_ts, e_ts] + others

def parse_fault(fault_row):
    # ignore historical status, assuming the alternative is predicted
    return fault_row[:-1]

