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

import http.client
import json
import logging
import re
import socket
import sys
from datetime import datetime, timedelta
from math import log

import numpy
import pandas
from delphixpy.v1_8_0.exceptions import HttpError, RequestError
from delphixpy.v1_8_0.delphix_engine import DelphixEngine
from delphixpy.v1_8_0.web.analytics import analytics
from delphixpy.v1_8_0.web.service.time import time
from delphixpy.v1_8_0.web.system import system

from dxanalyze.dxdata.dataprocessing import create_dataframes
from dxanalyze.dxlogging import print_error
from dxanalyze.dxlogging import print_message

from dxanalyze.dxdata.dxtime import convert_from_utc
from dxanalyze.dxdata.dxtime import convert_to_utc
from dxanalyze.dxdata.dxtime import make_iso_timestamp

__analytic_map = {
   "cpu": {
      "ref": None,
      "function": "process_cpu"
   },
   "network": {
      "ref": None,
      "function": "process_network"
   },
   "disk": {
      "ref": None,
      "function": "process_io"
   },
   "nfs": {
      "ref": None,
      "function": "process_io"
   },
   "iscsi": {
      "ref": None,
      "function": "process_io"
   }
}
__engineobject = None
__engine_time_zone = None
__current_time = None
__engine_name = None

def connect(f_engine_address, f_engine_username, f_engine_password):
   """
   Connect to the Delphix Engine and get time and analytic information
   f_engine_address: The Virtualization Engine's address (IP/DNS Name)
   f_engine_username: Username to authenticate
   f_engine_password: User's password
   """

   logger = logging.getLogger()

   try:
      global __engineobject
      global __engine_time_zone
      global __current_time
      global __engine_name
      __engineobject = DelphixEngine(f_engine_address,
                                     f_engine_username,
                                     f_engine_password,
                                     "DOMAIN")
      timeobj = time.get(__engineobject)
      __engine_time_zone = timeobj.system_time_zone
      __current_time = convert_from_utc(timeobj.current_time, __engine_time_zone)

      systemobj = system.get(__engineobject)
      __engine_name = systemobj.hostname

      for analytic_def in analytics.get_all(__engineobject):
         if analytic_def.name[8:] in __analytic_map:
            __analytic_map[analytic_def.name[8:]]["ref"] = analytic_def.reference
         

   except HttpError as e:
      if (e.status == 401):
            print_error('Wrong password or username for engine {}'.format(f_engine_address))
            logger.error('Wrong password or username for engine {}'.format(f_engine_address))
      else:
            print_error('An error occurred while authenticating to {}:\n{}'.format(f_engine_address. str(e.status)))
            logger.error('An error occurred while authenticating to {}:\n{}'.format(f_engine_address. str(e.status)))
      sys.exit(1)

   except RequestError as e:
      print_error(e)
      logger.error(str(e))
      sys.exit(1)

   except (http.client.HTTPException, socket.error) as ex:
      print_error("Issue when connecting to engine {}: \n {}".format(f_engine_address, str(ex)))
      logger.error("Issue when connecting to engine {}: \n {}".format(f_engine_address, str(ex)))
      sys.exit(1)

def get_engine_name():
   return __engine_name

def get_available_analytics():
   """
   Return a list of analytics required by dxanalyze and available in engine
   return: list of analytics
   """
   return [x for x in __analytic_map.keys() if __analytic_map[x]["ref"] is not None ]

def generate_pages(start_time_str, end_time_str):
   """
   Generator of pages using a time stamp between start_time tp end_time using 1 hour pages
   :param1 start_time_str: start time using engine timezone
   :param2 end_time_str: end time using engine timezone
   yield: touple of start and end time in iso format in zulu time zone for each page
   """

   logger = logging.getLogger()
   logger.debug("start time {} end time {}".format(start_time_str, end_time_str))
   delta1h = timedelta(minutes=60)
   start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
   end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")

   while(start_time < end_time):
      end_page = start_time + delta1h
      logger.debug("start date {} end date {}".format(start_time, end_page))
      if end_page > end_time:
         end_page = end_time
      yield (str(start_time), str(end_page))
      start_time = end_page


def process_analytics(analytic_name, start_time=None, end_time=None, resolution=60):
   """
   Get data from engine for particular analytic name, start time, end time and resolution
   Gathered data will be converted into CSV like Pandas dataframe and converted into statistics

   :param1 analytic_name: analytic name ( cpu, disk, nfs, iscsi, network)
   :param2 start_time: start time in engine time zone
   :param3 end_time: end time in engine time zone
   :param4 resolution: data resolution (default 60), allowed values 1, 60
   return: dictonary of statistics per analytic (see create_dataframes from dataprocessing for details)
   """

   # check if resolution is in 1, 60 or 3600

   logger = logging.getLogger()

   if start_time is None:
      ts = datetime.strptime(__current_time, '%Y-%m-%d %H:%M:%S')
      ts = ts - timedelta(days=7)
      start_time =  "{} {}".format(ts.date(), ts.time())
   else:
      m = re.match(r'(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d)', start_time)
      if m is None:
         print_error("Start time {} is not matching required format - YYYY-MM-DD HH24:MI:SS")
         logger.error("Start time {} is not matching required format - YYYY-MM-DD HH24:MI:SS")
         exit(1)

   if end_time is None:
      end_time = __current_time
   else:
      m = re.match(r'(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d)', end_time)
      if m is None:
         print_error("Enf time {} is not matching required format - YYYY-MM-DD HH24:MI:SS")
         logger.error("End time {} is not matching required format - YYYY-MM-DD HH24:MI:SS")
         exit(1)

   totaldata = pandas.DataFrame()

   for (st, et) in generate_pages(start_time, end_time):
      st_iso = make_iso_timestamp(convert_to_utc(st, __engine_time_zone))
      et_iso = make_iso_timestamp(convert_to_utc(et, __engine_time_zone))
      d = analytics.get_data(__engineobject, __analytic_map[analytic_name]["ref"], resolution=resolution, start_time=st_iso, end_time=et_iso)
      function_to_call = globals()[__analytic_map[analytic_name]["function"]]
      csvdata = function_to_call (d.to_dict()["datapointStreams"])
      totaldata = totaldata.append(csvdata)
   
   if not totaldata.empty:
      stats = create_dataframes(analytic_name, totaldata)
   else:
      print_error("There is no data collected for {}".format(analytic_name))
      stats = []
   return stats


def process_cpu(datapoint_streams):
   """
   Process a CPU data retured by engine
   :param1 datapoint_streams: will have 1 stream with CPU data if there will be any data to process
   return: dataframe with #timestamp and util column
   """
   cpu = None
   for stream in datapoint_streams:
      cpu = pandas.DataFrame(stream["datapoints"])
      cpu["util"] = (cpu["user"] + cpu["kernel"]) / (cpu["idle"] + cpu["user"] + cpu["kernel"]) * 100
      cpu["util"].replace(numpy.inf, 0, inplace=True)
      cpu = fix_timestamp(cpu)
   return cpu

def process_network(datapoint_streams):
   """
   Process a network data retured by engine
   :param1 datapoint_streams: will have 2 streams with inbound and outbound data if there will be any data to process
   return: dataframe with #timestamp, inbound and outbound column plus 1 column per interface [B/s]
   """

   nic_list = []
   network = None
   for stream in datapoint_streams:
      if network is None:
         network = pandas.DataFrame(stream["datapoints"])
      else:
         t = pandas.DataFrame(stream["datapoints"])
         network = pandas.merge(network, t, on="timestamp", how="inner", suffixes=("", "_{}".format(stream["networkInterface"])))
         nic_list.append(stream["networkInterface"])

   # sum through all network interfaces
   for name in nic_list:
      network["inBytes"] = network["inBytes"] + network["inBytes_{}".format(name)]
      network["outBytes"] = network["outBytes"] + network["outBytes_{}".format(name)]

   if network is not None:
      network = fix_timestamp(network)
   return network





def fix_timestamp(dataframe):
   """
   Convert timestamp from UTC into engine timezone and from ISO to format expected by graphs
   :param1 dataframe: data frame to process
   return: dataframe with converted timestamp column
   """
   dataframe["timestamp"] = dataframe["timestamp"].apply(convert_from_utc, args=(__engine_time_zone,))
   dataframe = dataframe.rename(columns={"timestamp": "#timestamp"}) 
   return dataframe


def process_io(datapoint_streams):
   """
   Process a IO (nfs, disk, iscsi) data retured by engine
   :param1 datapoint_streams: will have 2 streams with read and write data if there will be any data to process
   return: dataframe with #timestamp, read and write columns for number of ops, latency and throughput [MB/s]
   """
   io = None
   for stream in datapoint_streams:
      if io is None:
         io = pandas.DataFrame(stream["datapoints"])
         io = io.rename(columns={"latency": "{}_latency".format(stream["op"]), 
                                       "throughput": "{}_throughput".format(stream["op"]),
                                       "count": "ops_{}".format(stream["op"])
                                       })
      else:
         t = pandas.DataFrame(stream["datapoints"])
         io = pandas.merge(io, t, on="timestamp", how="inner")
         io = io.rename(columns={"latency": "{}_latency".format(stream["op"]), 
                                    "throughput": "{}_throughput".format(stream["op"]),
                                    "count": "ops_{}".format(stream["op"])
                                 })

   if io is not None:
      if not "read_latency" in io.keys():
         io["read_latency"] =  0
         io["read_throughput"] = 0
      else:
         io["read_latency"] = io["read_latency"].map(calculate_latency)
         io["read_throughput"] = io["read_throughput"] / 1024 / 1024

      if not "write_latency" in io.keys():
         io["write_latency"] =  0
         io["write_throughput"] = 0
      else:
         io["write_latency"] = io["write_latency"].map(calculate_latency)
         io["write_throughput"] = io["write_throughput"] / 1024 / 1024
      io = fix_timestamp(io)
   else:
      io = pandas.DataFrame()
   #print(io)
   return io

def calculate_latency(latency_dict):
   """
   Calculate an average latency based on histogram
   :param1 latency_dict: latency histogram
   return: average latency in ms rounded to 2 digits
   """
   sumCount = 0
   sumLatency = 0
   latency = 0
   for key,value in latency_dict.items():
      if key == "< 10000":
         # There's a very low bucket marked < 10000.  We'll workaround this and set it equal to 1000
         key = 1000
      fkey = float(key)
      fvalue = float(value)
      if fkey > 0:
         sumCount += fvalue
         base = int(log(fkey)/log(10) + 0.00000001)
         sub = 10**(base-1) * 5
         partLatency = fkey + sub
         sumLatency += (partLatency * fvalue)
   if (sumLatency == 0) and (sumCount == 0):
      latency = None
   else:
      latency = sumLatency / sumCount
   if latency is not None:
      return round(latency/1000000, 2)
   else:
      return latency


   