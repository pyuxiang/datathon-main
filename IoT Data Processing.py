########### BASIC MANIPULATION ###########


#excel file reader function

import csv

def readcsv (namecsv):
    """
    Takes namecsv as string, output is a list (of lists).
    Each individual element is a string.
    """

    output = []

    with open(namecsv) as csvfile:
        file = csv.reader(csvfile)

        for row in file:
            output.append(row)
    
    return output


def makecsv (listoflists, filename):
    """
    Takes a list of lists and file name as input. creates and writes a csv file.
    Each individual element must be a string
    """

    with open (filename, "w") as csvfile:
        writefn = csv.writer(csvfile)
        
        for row in listoflists:
            writefn.writerow(row)
    #end

def dataconvert(lstoflsts):
    """
    Converts each data point in the list of lists to the appropriate type

    Urhhhhh...
    """
    

def lsttodict(lstoflsts):
    """
    Converts the data to a dictionary
    """
    output = {}

    #Use top row as headers
    headers = lstoflsts[0]

    for i in range(len(headers)):
        #dictionary initialisation
        output[headers[i]] = list(filter(lambda x: x[i],lstoflsts))[1:]

    return output
        


def truncatelst(lst, mode, *params):
    """
    Cuts out relevant data.
    Can be selected based on string or index (int inputs)
    Mode has flexible inputs.
    """
    output = []

    isindex = True #default
    if type(mode) == str and mode != "index":
        isindex = False

    indexes = params
    
    if isindex == False:
        indexes = () #overwrite index for initialisation
        
        for i in range(len(lst[0])):
            if lst[0][i] in params:
                indexes += (i,)

    #all converted to index

    owncpy = lst.copy()
    owncpy.append([x for x in range(len(lst))])
    owncpy.reverse()

    flip = list(map(list, zip(*owncpy)))

    #truncation
    output = list(filter(lambda x: x[0] in indexes, flip))
    output = list(map(list, zip(*output)))
    output.reverse()

    return output
    
    

def truncatedict(dic, *labels):
    """
    Cuts out relevant data.
    Selected based on the data labels (Can't use indexes)
    """
    keys = dic.keys()
    output = {}
    
    for label in labels:
        if label in keys:
            output[label] = dic[label]

        else:
            print("There is an invalid label") #fail check
        
    return output



####### SPECIFICS ########

def rectify_time(file, acc):
    """output is a list"""
    
    lst = readcsv(file)
    
    output = [lst[0]] + list(map(lambda x: x[0][:acc], lst[1:]))

    return output
            
    
