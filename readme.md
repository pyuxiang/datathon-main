# Datathon 2.0
## How to use
### yearly_batch_file_parser.py
1. Include csv data for each chiller in subdirectory starting with "/data/Chiller". Data come in sets of 4, i.e. conflow, evaflow, pm, temp. Naming convention: `"/Chiller <num>/<month> <chiller> <type>.csv"`
2. Open `yearly_batch_file_parser.py` and change mins_delta if needed.
3. Run module.

### fault_logs_parser.py
1. Include all csv fault logs in subdirectory "/data/Fault Logs". Naming convention: `"/Chiller <num>/<month> <chiller> <type>.csv"`
2. Open `fault_logs_parser.py` and change mins_delta if needed.
3. Run module.

### training_dataset_generator.py
1. Run this last, after `yearly_batch_file_parser.py` and `fault_logs_parser.py`.
2. Open `training_dataset_generator.py` and change mins_delta if needed.
3. Run module.
4. When prompted for certain values:
    - Chiller stats dir: path, or blank for default ("60")
    - Error logs dir: path, or blank for default ("60_fault")
    - Chiller num: integer
    - Error type: string (case-sensitive)
    - Start and end datetimes: Year must be specified. Every successful integer input will prompt further details, e.g. month, day, hour, minute. All initial datetimes will be resolved to the earliest possible datetime within the year accounting for specified bounds, and matched to a narrower domain (start <= data < end) according to availability of data.

| Datetime   | User input | Dataset    |
|:----------:|:----------:|:----------:|
| 2017-08-01 |            |            |
| nil        | 2017-08-02 |            |
| 2017-08-03 |            | 2017-08-03 |
| nil        |            | 2017-08-04 |       
| 2017-08-05 |            | 2017-08-05 |
| nil        |            |            |
| 2017-08-06 | 2017-08-06 |            |


## TODO:
1. Write splitter to demarcate subdivision of training dataset / generate labels with the same dimension
2. Refactor code, especially for fault parser and data combinator
