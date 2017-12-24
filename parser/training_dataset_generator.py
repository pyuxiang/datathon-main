## Algo:
## 1. Ensure concatenated data for specified chiller and targeted fault exists
## 1a. If not existing, execute yearly_batch_file_parser.py and fault_logs_parser.py
## 2. Specify time period to generate relevant training data (start < end)
## 3.1. Blank error logs to fill as no error
## 3.2. Missing chiller statistics to fill with averaged values
## 3.3. start <= data_dt_1 < data_dt_2 <= end
## 4. Specify split between training and testing

from datetime import datetime as dt
import sys


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

    user_chiller = input("Specify chiller (1/2/3/4): ")
    if not is_int(user_chiller): sys.exit(1)
    user_chiller = int(user_chiller)
    user_error = input("Specify error: ") # INV MALFUNCTION
    user_start_dt = prompt_datetime("start") # Ensured dt object
    user_end_dt = prompt_datetime("end") # Ensured dt object
    user_ratio = input("Specify split ratio: ")
    if not is_ratio(user_ratio): sys.exit(1)
    return [user_chiller, user_error, user_start_dt, user_end_dt, user_ratio]

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
    
def main():
    prompt_user_inputs()
    
