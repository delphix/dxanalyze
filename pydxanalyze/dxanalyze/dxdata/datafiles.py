#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright (c) 2019 by Delphix. All rights reserved.
#

import re
from os import listdir, remove
from sys import exit
from os.path import isfile, join
from pandas import pandas

from dxanalyze.dxdata.dataprocessing import create_dataframes


# dictionary to keep a graph type ( cpu, network ) and a file name
files_mapping = {}
# list to keep a list of analytics to process
analytics_to_process = []

# dictionary to hold engine and cpu mapping files
engine_cpufile_mapping = {}
# dictionary to hold engine and network mapping files
engine_networkfile_mapping = {}
# dictionary to hold engine and throughput test mapping files
engine_throughputtestfile_mapping = {}
## list to keep list of summary data for farmanalyze
#engine_dict_list = []

def test_dir(directory_name):
    """
    Check if an directory is writeble. Exit with error if it's not
    :param1 directory_name: Name of the directory to check
    """
    try:
        testfilename = join(directory_name, "test.lck")
        testfile = open(testfilename, 'w')
        testfile.close()
        remove(testfilename)
    except OSError as e:
        print("Can't write into directory {}".format(directory_name))
        print(str(e))
        exit(1)



def detect_files(directory_name, engine_name):
    """
    Detect a data file in directory_name and populate a files_mapping dict
    :param1 directory_name: Name of the directory to scan
    :param2 engine_name:    Name of the Delphix engine ( file name prefix )
    """

    try:
        for name in listdir(directory_name):
            reout = re.match(r'{0}-analytics-(\bcpu\b|\bnfs\b|\bnetwork\b|\bdisk\b|\biscsi\b)-raw\.csv'.format(engine_name), name) 
            if reout is not None:
                files_mapping[reout.group(1)] = join(directory_name, name)
                analytics_to_process.append(reout.group(1))
    except FileNotFoundError as e:
        print("Can't find datadir directory {}".format(directory_name))
        print(str(e))
        exit(-1)

def detect_farmanalyze_files(directory_name):
    """
    Detect a data file in directory_name and populate a files_mapping dict
    :param1 directory_name: Name of the directory to scan
    """

    try:
        for name in listdir(directory_name):
            reout = re.match(r'(\b.*?\b)-analytics-(\bcpu\b|\bnetwork\b|\bthroughputtest\b)-aggregated\.csv', name)
            if reout is not None:
                # reout.group(1) : cpu/network filename, reout.group(2) = cpu/network
                if reout.group(2) == "cpu":
                    engine_cpufile_mapping[reout.group(1)] = join(directory_name, name)
                elif reout.group(2) == "network":
                    engine_networkfile_mapping[reout.group(1)] = join(directory_name, name)
                #elif reout.group(2) == "throughputtest":
                #    engine_throughputtestfile_mapping[reout.group(1)] = join(directory_name, name)
        for name in listdir(directory_name):
            if name.lower() == 'all_nt.csv':
                engine_throughputtestfile_mapping['all'] = join(directory_name, name)                    
    except FileNotFoundError as e:
        print("Can't find datadir directory {}".format(directory_name))
        print(str(e))
        exit(-1)
    return engine_cpufile_mapping,engine_networkfile_mapping,engine_throughputtestfile_mapping

def get_files_mapping():
    return files_mapping

def get_analytics_to_process():
    return analytics_to_process

def process_file(analytic_name, files_mapping):
    """
    Function is reading a csv file and creating a list of statistics to process
    :param1 analytic_name: name of the analytic to process
    :param2 files_mapping: dict with files to analytic mapping
    Return a list of dict { stat: Pandas Dataframe}
    """

    try:
        file_name = files_mapping[analytic_name]
        csvdata = pandas.read_csv(file_name)
        stats = create_dataframes(analytic_name, csvdata)
        return stats
    except KeyError as k:
        print("Can't find file mapping for analytics {}".format(analytic_name))
        print(str(k))
        exit(-1)
    except FileNotFoundError as e:
        print("Can't open a file {}".format(file_name))
        print(str(e))
        exit(-1)


