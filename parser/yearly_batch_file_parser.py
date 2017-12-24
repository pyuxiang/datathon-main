# TODO:
# 1. Match data rows during monthly concatenation (DONE)
# 2. Fill intermediate blanks in data with average of surrounding values?

# Important assumptions:
# 1. Individual files start from 12:00:00 AM
# 2. Data files must be contiguous, e.g. July -> August
# 3. Filenames of format: "/Chiller <num>/<month> <chiller> <type>.csv"


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
    concat_monthly_data()
    print("Concatenating yearly data.")
    concat_yearly_data()
    print("Job completed.")
    if clean_temp_files: clear_temp_dir()
    print("Artifacts cleaned.")
    return



######################
##  USER VARIABLES  ##
######################

# Remember to also look through validation logic

clean_temp_files = True
custom_suffix    = ""
mins_delta       = 60   # 60 == 1 hour interval

def mean_data(parsed_data): # For large time intervals, e.g. >= 6 hours
    transpose = lambda x: list(map(list,zip(*x)))
    return list(map(statistics.mean, transpose(parsed_data)))

def median_data(parsed_data): # For small time intervals, e.g. < 6 hours
    transpose = lambda x: list(map(list,zip(*x)))
    return list(map(statistics.median, transpose(parsed_data)))

average_data     = median_data if mins_delta < 360 else mean_data # aggregation method



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

parsed_prefix = "P "

def tokenize_raw_filename(filename):
    month, chiller, filetype = filename.split(" ")
    filetype = filetype[:-4] # remove .csv suffix
    return month, chiller, filetype

def parse_file(filename, mins_delta=60, file_dir=os.getcwd()):
    """ mins_delta specifies data duration interval """

    # Retrieve filetype and set parser and validator type
    month, chiller, filetype = tokenize_raw_filename(filename)
    parser_type = None
    if filetype == "temp":
        parser_type = parse_temp_row
        validate_row_data = validate_temp_row_data
    elif filetype in ("conflow", "evaflow"):
        parser_type = parse_flow_row
        validate_row_data = validate_flow_row_data
    elif filetype == "pm":
        parser_type = parse_power_row
        validate_row_data = validate_power_row_data
    else:
        raise Exception("Unknown filetype!")
    data_parser = lambda x: validate_row_data(parse_row_data(parser_type(x))) # Returns [bool, data_row]

    # File IO
    infile = open(file_dir + "\\" + filename)
    outfile = open(temp_dir + "\\" + parsed_prefix + filename, "w", newline="")
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Process header row
    header_row = next(reader)
    timestamp_header = ["year","month","day","hour","minute"]
    data_headers = parser_type(header_row)[1:]
    if filetype == "conflow": data_headers = list(map(lambda x: "con_" + x, data_headers))
    if filetype == "evaflow": data_headers = list(map(lambda x: "eva_" + x, data_headers))
    headers = timestamp_header + data_headers
    writer.writerow(headers)

    # Process data
    starting_datetime = None
    parsed_data = []
    for row in reader:
        if starting_datetime == None:
            starting_datetime = data_parser(row)[1][0] # ignore validation here
            starting_datetime = starting_datetime.replace(hour=0, minute=0) # Initialise datetime to 12:00:XX.X
        
        validation, current_row = data_parser(row)
        current_datetime = current_row[0]
        if current_datetime < starting_datetime + timedelta(minutes=mins_delta):
            if validation:
                parsed_data.append(current_row[1:])
        else:
            # Exceeded time interval -- Write to file
            if parsed_data:
                row_data = unpack_timestamp(starting_datetime) + average_data(parsed_data)
                writer.writerow(row_data)

            # Reset and continue appending
            starting_datetime += timedelta(minutes=mins_delta) # Go to next time group
            parsed_data = []
            if validation:
                parsed_data.append(current_row[1:])

    # Last row is not stored
    if parsed_data:
        row_data = unpack_timestamp(starting_datetime) + average_data(parsed_data)
        writer.writerow(row_data)

    infile.close()
    outfile.close()



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
    ts = parse_timestamp(data_row[0]) # First column is always timestamp
    others = list(map(float, data_row[1:]))
    return [ts] + others

def parse_temp_row(temp_row):
    """ Get relevant temp columns """
    return temp_row[:1] + temp_row[6:]

def parse_power_row(power_row):
    """ Get relevant power columns """
    return power_row[:1] + power_row[6:]

def parse_flow_row(flow_row):
    """ Get relevant flow columns """
    return flow_row[:1] + flow_row[5:10]

########################
##  Validation Logic  ##
########################

## Be extremely careful with the quantities validated!
## Do not validate based on median quantities, but raw data instead

def validate_temp_row_data(data_row):
    return [True, data_row]

def validate_power_row_data(data_row):
    return [True, data_row]

def validate_flow_row_data(data_row):
    flowRate, flowSpeed, totalFlowRate, positiveTotalFlow, positiveTotalFlowDecimal = data_row[1:]
    if 0 <= flowRate: # < 100 ?
        if 0 <= flowSpeed: # < 10?
            if totalFlowRate >= 0:
                if positiveTotalFlow >= 0:
                    if positiveTotalFlowDecimal >= 0:
                        return [True, data_row]
    return [False, data_row]




###########################
##  MONTHLY FILE CONCAT  ##
###########################

# Work in batches of four
# One batch = ["July Chiller1 conflow.csv",
#              "July Chiller1 evaflow.csv",
#              "July Chiller1 pm.csv",
#              "July Chiller1 temp.csv"]

def concat_monthly_data():

    # Build list of csv files in data directory
    listed_dir = list(filter(lambda x: x.startswith("Chiller"), os.listdir(data_dir)))
    print("List of directories:\n{}".format(listed_dir))    
    for chiller in listed_dir:
        if not chiller.startswith("Chiller"): continue # ignore fault logs
        chiller_dir = data_dir + "\\" + chiller
        print("Current directory\n{}\n".format(chiller_dir))
        
        csv_files = []
        for file in os.listdir(chiller_dir):
            if os.path.isfile(chiller_dir + "\\" + file) and file.endswith(".csv"):
                csv_files.append(file)

        # Quick check for batch consistency
        if len(csv_files) % 4 != 0: raise Exception("Wrong number of csv files ({})!".format(len(csv_files)))
        csv_files.sort() # Ensure running order
        # Check filenames consistent before creating batches
        for i in range(0,len(csv_files),4):
            expect_filename = csv_files[i]
            expect_month, expect_chiller, expect_filetype = tokenize_raw_filename(expect_filename)
            filetypes = [expect_filetype]
            for j in range(i+1, i+4):
                curr_filename = csv_files[j]
                curr_month, curr_chiller, curr_filetype = tokenize_raw_filename(curr_filename)
                if curr_month != expect_month:
                    raise Exception("Different months {} and {}!".format(expect_month, curr_month))
                if curr_chiller != expect_chiller:
                    raise Exception("Different chillers {} and {}!".format(expect_chiller, curr_chiller))
                if curr_filetype in filetypes:
                    raise Exception("Same filetype {} and {}!".format(expect_filetype, curr_filetype))
                else:
                    filetypes.append(curr_filetype)

        print("List of files:\n{}\n".format(csv_files))
        while csv_files:

            # Create individual batch
            batch = []
            for i in range(4):
                batch.append(csv_files.pop(0))

            # Display output filename
            month, chillerid, filetype = tokenize_raw_filename(batch[0])
            outfilename = month + " " + chillerid + ".csv"
            print("Current batch: {}".format(outfilename))
            
            for i in range(4):
                print("Parsing: {}".format(batch[i]))
                parse_file(batch[i], mins_delta, chiller_dir)
            parsed_batch = list(map(lambda x: parsed_prefix + x, batch))
            
            print("Parsing completed")
            concat_monthly(parsed_batch)
            print("Concatenation completed")
    
            if clean_temp_files:
                for item in parsed_batch:
                    os.remove(temp_dir + "\\" + item)
                print("Batch artifacts cleaned\n")

def concat_monthly(filenames):
    infiles = list(map(lambda x: open(temp_dir + "\\" + x), filenames)) # batch open files
    readers = list(map(lambda x: csv.reader(x), infiles))
    
    month, chiller = filenames[0].split(" ")[1:3]
    outfilename = month + " " + chiller + ".csv"
    outfile = open(temp_dir + "\\" + outfilename, "w", newline="")
    writer = csv.writer(outfile)

    def concat_rows(rows):
        all_header = rows[0][:5] # timestamp of size 5 ["year","month","day","hour","minute"]
        for row in rows:
            for i in range(5, len(row)): # ignore timestamp
                all_header.append(row[i])
        return all_header

    def retrieve_datetime_row(rows):
        ts_row = [[int(x) for x in row] for row in map(lambda x: x[:5], rows)]
        return list(map(lambda ts: datetime(ts[0],ts[1],ts[2],ts[3],ts[4]),ts_row))
        
    # Build header
    headers = list(map(next, readers)) # Batch iteration
    writer.writerow(concat_rows(headers))

    # Write individual data rows
    # Note: StopIteration terminates iteration at current reading point, i.e. len(data_rows) < 5
    data_rows = list(map(next, readers))
    try:
        while len(data_rows) == 4:
            # All dt must be consistent before writing to writer (currently removing incomplete rows)
            dt_row = retrieve_datetime_row(data_rows)
            if len(set(dt_row)) != 1: # dt not equal
                max_dt = max(dt_row)
                for i in range(len(dt_row)):
                    while dt_row[i] < max_dt:
                        data_rows = data_rows[:i] + [next(readers[i])] + data_rows[i+1:]
                        dt_row = retrieve_datetime_row(data_rows)

            writer.writerow(concat_rows(data_rows))
            data_rows = list(map(next, readers))
            
    except StopIteration:
        pass
    
    list(map(lambda x: x.close(), infiles)) # batch close files, list to evaluate mapping
    outfile.close()




##########################
##  YEARLY FILE CONCAT  ##
##########################

def concat_yearly_data():

    def tokenize_concat_filename(filename):
        month, chiller = filename.split(" ") # August Chiller1.csv
        chiller = chiller[:-4] # remove .csv suffix
        return [month, chiller]

    get_month = lambda x: tokenize_concat_filename(x)[0]
    get_chiller_id = lambda x: tokenize_concat_filename(x)[1]
    
    csv_files = []
    for file in os.listdir(temp_dir):
        if os.path.isfile(temp_dir + "\\" + file) \
               and file.endswith(".csv") \
               and not file.startswith(parsed_prefix):
            csv_files.append(file)

    # Separate datasets based on chiller id
    chiller_ids = set(map(get_chiller_id, csv_files))
    month_order = {"January":1, "February":2, "March":3, "April":4,
                   "May":5, "June":6, "July":7, "August":8,
                   "September":9, "October":10, "November":11, "December":12}
    
    for chiller_id in chiller_ids:
        chiller_dataset = list(filter(lambda x: get_chiller_id(x) == chiller_id, csv_files))
        chiller_dataset.sort(key=lambda x: month_order[get_month(x)])
        print(chiller_dataset)
        concat_yearly(chiller_dataset)
    
def concat_yearly(dataset):
    
    outfilename = dataset[0].split(" ")[1]
    outfile = open(agg_dir + "\\" + outfilename, "w", newline="")
    writer = csv.writer(outfile)

    # Retrieve header only
    with open(temp_dir + "\\" + dataset[0]) as infile:
        header = next(csv.reader(infile))
        writer.writerow(header)

    # Retrieve data only
    for filename in dataset:
        with open(temp_dir + "\\" + filename) as infile:
            reader = csv.reader(infile)
            next(reader)
            for row in reader:
                writer.writerow(row)
                
    outfile.close()

if __name__ == "__main__":
    main()

