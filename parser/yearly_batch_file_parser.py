import os, sys
import csv
from datetime import *

def concat_yearly_data():
    # August Chiller1.csv
    file_path = os.getcwd() + "\\output\\" # Get concat data
    concat_directory = os.listdir(file_path)
    chiller1_files = list(filter(lambda x: 
    

def main():

    # Build list of csv files in directory
    file_path = os.getcwd() + "\\"
    sys.path.insert(0, file_path) # Add path to env
    
    csv_files = []
    for file in os.listdir(file_path):
        if os.path.isfile(file_path + file) and file.endswith(".csv") and \
               (not file.startswith("parsed")):
            csv_files.append(file)

    # Work in batches of four
    # One batch = ["July Chiller1 conflow.csv",
    #              "July Chiller1 evaflow.csv",
    #              "July Chiller1 pm.csv",
    #              "July Chiller1 temp.csv"]

    # Check if multiples of four
    if len(csv_files) % 4 != 0:
        raise Exception("Wrong number of csv files ({})!".format(len(csv_files)))

    csv_files.sort() # Ensure running order
    # Check filenames consistent before creating batches
    for i in range(0,len(csv_files),4):
        expect_filename = csv_files[i]
        expect_month, expect_chiller, expect_filetype = tokenize_filename(expect_filename)
        filetypes = [expect_filetype]
        for j in range(i+1, i+4):
            curr_filename = csv_files[j]
            curr_month, curr_chiller, curr_filetype = tokenize_filename(curr_filename)
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

        # Parse all files in batch
        batch = []
        for i in range(4):
            batch.append(csv_files.pop(0))
        print("Parsing:\n{}".format(batch))
        for i in range(4):
            parse_file(batch[i])
        parsed_batch = list(map(lambda x: "parsed " + x, batch))
        
        # Assuming data is consistent and
        # each parsed file has equal number of rows
        print("Concatenating:\n{}".format(parsed_batch))
        if "output" not in os.listdir(): os.mkdir("output") # Create dir if not existing
        concat_files(parsed_batch)
        print("Concatenation completed")
        for item in parsed_batch:
            os.remove(item)
        print("Artifacts cleaned\n")
        

def concat_files(filenames):
    infiles = list(map(lambda x: open(x), filenames)) # batch open files
    readers = list(map(lambda x: csv.reader(x), infiles))
    
    month, chiller = filenames[0].split(" ")[1:3]
    outfilename = month + " " + chiller + ".csv"
    outfile = open("output/" + outfilename, "w", newline="")
    writer = csv.writer(outfile)

    def concat_rows(rows):
        all_header = rows[0][:5] # timestamp of size 5 ["year","month","day","hour","minute"]
        for row in rows:
            for i in range(5, len(row)): # ignore timestamp
                all_header.append(row[i])
        return all_header
        
    # Build header
    headers = list(map(next, readers)) # Batch iteration
    writer.writerow(concat_rows(headers))

    # Write individual data rows
    data_rows = list(map(next, readers))
    while data_rows:
        writer.writerow(concat_rows(data_rows))
        data_rows = list(map(next, readers))
    
    map(lambda x: x.close(), infiles) # batch close files
    outfile.close()

def tokenize_filename(filename):
    month, chiller, filetype = filename.split(" ")
    filetype = filetype[:-4] # remove .csv suffix
    return month, chiller, filetype


def parse_file(filename, mins_delta = 60):
    """ mins_delta specifies data duration interval """

    # Retrieve filetype and set parser_type
    month, chiller, filetype = tokenize_filename(filename)
    parser_type = None
    if filetype == "temp":
        parser_type = parse_temp_row
    elif filetype in ("conflow", "evaflow"):
        parser_type = parse_flow_row
    elif filetype == "pm":
        parser_type = parse_power_row
    else:
        return Exception("Unknown filetype!")
    data_parser = lambda x: parse_row_data(parser_type(x))

    # CSV File IO
    infile = open(filename)
    # output file name "parsed August Chiller1 temp.csv"
    outfile = open("parsed " + filename, "w", newline="")
    data = csv.reader(infile)
    writer = csv.writer(outfile)

    # Parser and writer
    starting_datetime = None
    parsed_data = []
    count = 0
    first_row_skipped = False
    for row in data:
        if not first_row_skipped:
            headers = ["year","month","day","hour","minute"] + parser_type(row)[1:]
            writer.writerow(headers)
            first_row_skipped = True
            continue
        if starting_datetime == None:
            starting_datetime = data_parser(row)[0]
            # Initialise datetime to 12:00:00.0
            starting_datetime = starting_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        
        current_row = data_parser(row)
        current_datetime = current_row[0]
        if current_datetime < starting_datetime + timedelta(minutes=mins_delta):
            count += 1
            parsed_data.append(current_row[1:])
        else:
            ## Exceeded time interval
            row_data = unpack_timestamp(starting_datetime) + average_data(parsed_data, count)
            writer.writerow(row_data)
            starting_datetime += timedelta(minutes=mins_delta) # Go to next time group

    ## Last row is not stored
    row_data = unpack_timestamp(starting_datetime) + average_data(parsed_data, count)
    writer.writerow(row_data)

    infile.close()
    outfile.close()

def average_data(parsed_data, count):
    averaged_data = []
    for i in range(len(parsed_data[0])):
        total_sum = sum(map(lambda x: x[i], parsed_data))
        averaged_data.append(total_sum/count)
    return averaged_data




## PARSING LOGIC

def unpack_timestamp(dt):
    """ Unpacks datetime object as list up to minute resolution """
    return [dt.year, dt.month, dt.day, dt.hour, dt.minute]

def parse_timestamp(timestamp):
    ### Converts '2017-08-01 00:02:09.937' to datetime object
    year, month, day = map(int, timestamp[:10].split("-"))
    hour, minute, seconds = map(int, timestamp[11:19].split(":"))
    microseconds = int(timestamp[20:]) # ignore '.'
    return datetime(year, month, day, hour, minute, seconds, microseconds)

def parse_row_data(data_row):
    """ Parse relevant data columns after initial parse """
    # First column is always timestamp
    ts = parse_timestamp(data_row[0])
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


if __name__ == "__main__":
    main()


