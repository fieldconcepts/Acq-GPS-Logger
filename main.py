'''
ACQ Timestamp Logger
Version 1.0

Script that watched a folder for new dicrete MIRF files.
Writes Rcd#, source, GPS timestamp info to a logfile in a custom format
Continusly monitors folder and updates log.txt when new files arrive in the folder


19/10/2022
'''
#--------------------------------------
# Import libraries
#--------------------------------------
import mirf
import os.path
import re
import time
from datetime import datetime


#--------------------------------------
# Helper functions
#--------------------------------------

def generate_timestamp_line(directory, file):
    f = mirf.MirfFile(directory + os.path.sep + file)
    timestamp = f.time
    timestamp_formatted = timestamp.strftime("%Y,%m,%d,%H,%M,%S.%f")
    return "{},{},{}".format(f.record_num,f.source,timestamp_formatted)


#--------------------------------------
# Setup and configuration
#--------------------------------------

# source folder
source_location = r"221020"
source_dir = r"C:\Jobs\DAS Shoot Out Prep 221017" + os.path.sep + source_location 
print("Source directory: {}".format(source_dir))

# target file
now = datetime.now().strftime('%Y%m%d%H%M%S')
target_file = "gps_log_" + now + ".txt"
target_dir = r"C:\Users\asltestuser\Desktop" + os.path.sep + target_file
print("Tagret directory: {}".format(target_dir))

# Pattern for retrieving rcd number from the filename
file_pattern = re.compile("f_([0-9]{6})\.rcd")

# Refresh rate (seconds)
refresh_rate = 5


#--------------------------------------
# Process inital files in source folder
#--------------------------------------

#filter out only the f_xxxxxx.rcd files and create a dict
current_files = dict ([(f, None) for f in os.listdir(source_dir) if re.match(file_pattern, f)])

# create and new text file to write to
log = open(target_dir, "a")

# print out the fomratted stirng
for file in current_files.keys():
    line_formatted = generate_timestamp_line(source_dir, file)
    
    # write new line to log
    print(line_formatted)
    log.write(line_formatted + "\n")

# close log
log.close()


#--------------------------------------
# Monitor source folder for new files
#--------------------------------------

while True:
  
    #check for new files every 5 seconds
    time.sleep (refresh_rate)
    
    # create a dictionary of filtered discretercd files
    new_files = dict ([(f, None) for  f in os.listdir(source_dir) if re.match(file_pattern, f)])
    
    # compare the difference between the starting list and the current list
    added_files = [f for f in new_files if not f in current_files]

    if added_files:
        print("New file found!")

        log = open(target_dir, "a")

        for file in added_files:
            line_formatted = generate_timestamp_line(source_dir, file)
            print(line_formatted)
            
            # append new line to log
            log.write(line_formatted + "\n")
        
        log.close()

    # update the current dictionary of files
    current_files = new_files



