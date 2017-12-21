import csv
from datetime import *

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

