## Algo:
## 1. Ensure concatenated data for specified chiller and targeted fault exists
## 1a. If not existing, execute yearly_batch_file_parser.py and fault_logs_parser.py
## 2. Specify time period to generate relevant training data (start < end)
## 3.1. Blank error logs to fill as no error
## 3.2. Missing chiller statistics to fill with averaged values
## 3.3. start <= data_dt_1 < data_dt_2 < end
## 4. Specify split between training and testing (NOT DONE)



mins_delta = 60
neg_err = 0


#####################
##  MAIN FUNCTION  ##
#####################

from datetime import datetime as dt, timedelta as td
import sys, os
import csv
import h5py
from numpy import genfromtxt

def main():

    initialise_dir()
    stats_path, error_path, ch, err, s_dt, e_dt, ratio = prompt_user_inputs()
    stats_dir = out_dir + "\\" + stats_path
    error_dir = out_dir + "\\" + error_path

    # Open relevant files
    if not "Chiller{}.csv".format(ch) in os.listdir(stats_dir):
        print("Chiller{}.csv does not exist in \\{}!".format(ch, stats_path))
        sys.exit(1)

    if not "Chiller{} {}.csv".format(ch, err) in os.listdir(error_dir):
        print("Chiller{} {}.csv does not exist in \\{}!".format(ch, err, error_path))
        sys.exit(1)

    stat_filepath = stats_dir + "\\Chiller{}.csv".format(ch)
    error_filepath = error_dir + "\\Chiller{} {}.csv".format(ch, err)
    data_filepath = out_dir + "\\Chiller{} {} {}-{} DATA{}.csv".format(ch, err, parse_dt_str(s_dt), parse_dt_str(e_dt), mins_delta)
    label_filepath = out_dir + "\\Chiller{} {} {}-{} LABEL{}.csv".format(ch, err, parse_dt_str(s_dt), parse_dt_str(e_dt), mins_delta)
    filepaths = [stat_filepath, error_filepath, data_filepath, label_filepath]

    generate_dataset(filepaths, s_dt, e_dt, mins_delta)
    generate_h5py(data_filepath, "data")
    generate_h5py(label_filepath, "label")
    
def generate_h5py(filepath, name):
    with h5py.File(out_dir + "\\{}.h5".format(name), "w") as file:
        file['data'] = genfromtxt(filepath, delimiter=",")

#################
##  DIRECTORY  ##
#################

error_suffix = "_fault"
out_path = "out"

root_dir = os.getcwd() + "\\..\\"
out_dir = root_dir + out_path

def initialise_dir():
    if out_path not in os.listdir(root_dir):
        os.mkdir(out_dir)


####################
##  USER PROMPTS  ##
####################

def is_int(str_num):
    try:
        int(str_num)
        return True
    except ValueError:
        print("{} is not an integer!".format(str_num))
        return False

def is_ratio(str_float):
    try:
        num = float(str_float)
        if 0 < num <= 1:
            return True
        else:
            print("{} is not within (0,1]!".format(str_float))
            return False
    except ValueError:
        print("{} is not a float!".format(str_float))
        return False

def prompt_user_inputs():

    print("\nSPECIFY DIRECTORY")
    stats_path = input("Specify chiller stats dir: ")
    if not stats_path:
        stats_path = str(mins_delta) # e.g. 60
    error_path = input("Specify error logs dir: ")
    if not error_path:
        error_path = str(mins_delta) + error_suffix # e.g. 60_fault

    print("\nSPECIFY STATISTICS")
    user_chiller = input("Specify chiller (1/2/3/4): ")
    if not is_int(user_chiller): sys.exit(1)
    user_chiller = int(user_chiller)
    user_error = input("Specify error: ") # INV MALFUNCTION

    print("\nSPECIFY DATASET")
    user_start_dt = prompt_datetime("start") # Ensured dt object
    user_end_dt = prompt_datetime("end") # Ensured dt object
    if user_start_dt >= user_end_dt:
        print("Invalid period!")
        sys.exit(1)
    user_ratio = input("Specify split ratio: ")
    if not is_ratio(user_ratio): sys.exit(1)
    return [stats_path, error_path, \
            user_chiller, user_error, \
            user_start_dt, user_end_dt, user_ratio]

def prompt_datetime(name):
    
    user_year = input("Specify {} year: ".format(name)) # mandatory
    if not (user_year and is_int(user_year)): sys.exit(1)
    user_dt = dt(int(user_year), 1, 1)

    user_month = input("Specify {} month: ".format(name))
    if user_month and is_int(user_month):
        user_dt = user_dt.replace(month=int(user_month))
        
        user_day = input("Specify {} day: ".format(name))
        if user_day and is_int(user_day):
            user_dt = user_dt.replace(day=int(user_day))
            
            user_hour = input("Specify {} hour: ".format(name))
            if user_hour and is_int(user_hour):
                user_dt = user_dt.replace(hour=int(user_hour))
                
                user_minute = input("Specify {} minute: ".format(name))
                if user_minute and is_int(user_minute):
                    user_dt = user_dt.replace(minute=int(user_minute))
    
    return user_dt                
    
def parse_dt_str(dt):
    """ Unpacks datetime object as list up to minute resolution """
    return "{}{:02}{:02}-{:02}{:02}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)

def unpack_timestamp(dt):
    """ Unpacks datetime object as list up to minute resolution """
    return [dt.year, dt.month, dt.day, dt.hour, dt.minute]


#####################
##  CONCATENATION  ##
#####################

# Laborious reading, to avoid clogging memory for large filesizes

# Match datetimes
def read_as_dt(row):
    return dt(*map(int, row[:5]))

def get_stat_data(row):
    return list(map(float, row[5:]))

def get_error_data(row): # assuming float written to csv remains a float
    return list(map(int, row[5:]))

get_error_data = get_stat_data

def pairwise_sub(row1, row2):
    return list(map(lambda x: x[0]-x[1], zip(row1, row2)))

def pairwise_add(row1, row2):
    return list(map(lambda x: x[0]+x[1], zip(row1, row2)))

def generate_dataset(filepaths, s_dt, e_dt, mins_delta):

    stats_filepath, error_filepath, data_filepath, label_filepath = filepaths
    s_dt, e_dt = match_datetimes(stats_filepath, error_filepath, s_dt, e_dt)

    statfile = open(stats_filepath)
    errorfile = open(error_filepath)
    datafile = open(data_filepath, "w", newline="")
    labelfile = open(label_filepath, "w", newline="")
    
    stat_reader = csv.reader(statfile)
    error_reader = csv.reader(errorfile)
    data_writer = csv.writer(datafile)
    label_writer = csv.writer(labelfile)

    # Headers
    data_writer.writerow(next(stat_reader))
    label_writer.writerow(next(error_reader))
    
    # Go to s_dt
    curr_stat = next(stat_reader)
    while read_as_dt(curr_stat) != s_dt:
        curr_stat = next(stat_reader)
    curr_error = next(error_reader)
    while read_as_dt(curr_error) != s_dt:
        curr_error = next(error_reader)

    # Write data
    curr_dt = read_as_dt(curr_stat)
    while curr_dt < e_dt: # Ends when curr_dt == e_dt (latter already matched)
        try:
            next_stat = next(stat_reader)
            next_dt = read_as_dt(next_stat)
            step = (next_dt - curr_dt) // td(minutes=mins_delta)
            if step > 1:
                curr_data = get_stat_data(curr_stat)
                next_data = get_stat_data(next_stat)
                diff_data = pairwise_sub(next_data, curr_data) # Pairwise subtraction
                step_diff_data = list(map(lambda x: x/step, diff_data))
                for i in range(step):
                    data_writer.writerow(unpack_timestamp(curr_dt) + curr_data)
                    curr_dt += td(minutes=mins_delta)
                    curr_data = pairwise_add(curr_data, step_diff_data)
                curr_stat = next_stat
            else:
                data_writer.writerow(unpack_timestamp(curr_dt) + get_stat_data(curr_stat))
                curr_stat = next_stat
                curr_dt = read_as_dt(curr_stat)
        except StopIteration:
            break

    data_writer.writerow(unpack_timestamp(curr_dt) + get_stat_data(curr_stat)) # write last row

    # Write labels
    curr_dt = read_as_dt(curr_error)
    while curr_dt < e_dt:
        label_writer.writerow(unpack_timestamp(curr_dt) + get_error_data(curr_error))
        try:
            next_error = next(error_reader)
            next_dt = read_as_dt(next_error)
            step = (next_dt - curr_dt) // td(minutes=mins_delta)
            if step > 1:
                curr_dt += td(minutes=mins_delta)
                for i in range(step-1):
                    label_writer.writerow(unpack_timestamp(curr_dt) + [neg_err])
                    curr_dt += td(minutes=mins_delta)
                curr_error = next_error
            else:
                curr_error = next_error
                curr_dt = read_as_dt(curr_error)
        except StopIteration:
            break
                
    label_writer.writerow(unpack_timestamp(curr_dt) + get_error_data(curr_error))
    
    statfile.close()
    errorfile.close()
    datafile.close()
    labelfile.close()

def match_datetimes(stats_filepath, error_filepath, s_dt, e_dt):
    
    statfile = open(stats_filepath)
    errorfile = open(error_filepath)
    
    stat_reader = csv.reader(statfile)
    error_reader = csv.reader(errorfile)
    next(stat_reader)
    next(error_reader)    

    # Match starting datetimes
    stat_s_dt = read_as_dt(next(stat_reader))
    while stat_s_dt < s_dt: stat_s_dt = read_as_dt(next(stat_reader))
    error_s_dt = read_as_dt(next(error_reader))
    while error_s_dt < s_dt: error_s_dt = read_as_dt(next(error_reader))
    while stat_s_dt != error_s_dt:
        if stat_s_dt < error_s_dt:
            stat_s_dt = read_as_dt(next(stat_reader))
        else:
            error_s_dt = read_as_dt(next(error_reader))
    s_dt = stat_s_dt

    # Match ending datetimes
    selected_e_dt = None
    stat_e_dt = stat_s_dt
    while stat_e_dt < e_dt:
        try:
            selected_e_dt = stat_e_dt
            stat_e_dt = read_as_dt(next(stat_reader))
            error_e_dt = read_as_dt(next(error_reader))
            while stat_e_dt != error_e_dt:
                if stat_e_dt < error_e_dt:
                    stat_e_dt = read_as_dt(next(stat_reader))
                else:
                    error_e_dt = read_as_dt(next(error_reader))
            if stat_e_dt >= e_dt:
                break
        except StopIteration:
            break
        
    e_dt = selected_e_dt
    if e_dt == None:
        print("No valid points in dataset range!")
        sys.exit(1)
    
    statfile.close()
    errorfile.close()

    print("\nTime period:\nStart = {}\nEnd = {}\n".format(s_dt, e_dt))
    return s_dt, e_dt

if __name__ == "__main__":
    main()
