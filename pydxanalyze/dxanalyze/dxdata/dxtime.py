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
import logging
import pytz
from datetime import datetime, timedelta


def make_iso_timestamp(timestamp):
    """
    Convert timestamp YYYY-MM-DD HH24:MI:SS into ISO format
    :param1 timestamp: timestamp to convert 
    return: ISO timestamp
    """
    logger = logging.getLogger()
    logger.debug("timestamp %s" % timestamp)
    if re.match(r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d', timestamp):
       ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
       iso = str(ts.date()) + 'T' + str(ts.time()) + '.000Z'
    else:
       iso = None
 
    logger.debug("return iso format %s" % iso)
    return iso
 
 
def convert_using_offset(timestamp, src, dst, printtz):
    """
    Convert timestamp from src timezone to dst timezone using a offset
    limited to/from UTC
    :param1 timestamp: timestamp in ISO format or YYYY-MM-DD HH24:MI:SS
    :param2 src: source timezone,
    :param3 dst: dst timezone
    :param4 printtz: return string with timezone information
    return: converted timestamp
    """
    logger = logging.getLogger()
    logger.debug("timestamp %s" % timestamp)
    logger.debug("timezones %s %s" % (src, dst))
 
    m = re.match(r'(\d\d\d\d-\d\d-\d\d)T(\d\d:\d\d:\d\d)\.\d\d\dZ', timestamp)
    if m:
       ts = datetime.strptime(m.group(1) + ' ' + m.group(2),
                               '%Y-%m-%d %H:%M:%S')
    else:
       ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
 
    if (src == 'UTC'):
       offset = re.match(r'GMT([+|-]\d\d)\:(\d\d)', dst)
       if offset:
          offsethours = offset.group(1)
          dst_ts = ts + timedelta(hours=int(offsethours))
    elif (dst == 'UTC'):
       offset = re.match(r'GMT([+|-]\d\d)\:(\d\d)', src)
       if offset:
          offsethours = offset.group(1)
          dst_ts = ts - timedelta(hours=int(offsethours))
    else:
       return None
 
    ret_ts = str(dst_ts.date()) + ' ' + \
       str(dst_ts.time())
 
    if printtz is not None:
       ret_ts = ret_ts + ' ' + 'GMT' + offsethours
 
    return ret_ts
 
def convert_timezone(timestamp, src, dst, printtz):
    """
    Convert timestamp from src timezone to dst timezone
    :param1 timestamp: timestamp in ISO format or YYYY-MM-DD HH24:MI:SS
    :param2 src: source timezone,
    :param3 dst: dst timezone
    :param4 printtz: return string with timezone information
    return: converted timestamp
    """
    logger = logging.getLogger()
    logger.debug("timestamp %s" % timestamp)
    logger.debug("timezones %s %s" % (src, dst))
 
 
    m = re.match(r'(\d\d\d\d-\d\d-\d\d)T(\d\d:\d\d:\d\d)\.\d\d\dZ', timestamp)
    if m:
       ts = datetime.strptime(m.group(1) + ' ' + m.group(2),
                               '%Y-%m-%d %H:%M:%S')
    else:
       ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
 
    try:
       srctz = pytz.timezone(src)
       loc_ts = srctz.localize(ts)
       dsttz = pytz.timezone(dst)
       dst_ts = loc_ts.astimezone(dsttz)
       ret_ts = str(dst_ts.date()) + ' ' + \
          str(dst_ts.time())
       if printtz is not None:
          ret_ts = ret_ts + ' ' + str(dst_ts.tzname())
 
       return ret_ts
    except TypeError:
       return None
 
 
def convert_from_utc(timestamp, timezone, printtz=None):
    """
    Convert from UTC into timezone
    :param1 timestamp: timestamp in ISO format or YYYY-MM-DD HH24:MI:SS
    :param2: timezone: dst timezone,
    :param3: printtz: return string with timezone information
    return: converted timestamp
    """
    offset = re.match(r'GMT([+|-]\d\d)\:(\d\d)', timezone)
    if offset:
       return convert_using_offset(timestamp, 'UTC', timezone, printtz)
    else:
       return convert_timezone(timestamp, 'UTC', timezone, printtz)
 
 
def convert_to_utc(timestamp, timezone, printtz=None):
    """
    Convert to UTC from timezone
    :param1 timestamp: timestamp in ISO format or YYYY-MM-DD HH24:MI:SS
    :param2: timezone: src timezone,
    :param3: printtz: return string with timezone information
    return: converted timestamp
    """
    offset = re.match(r'GMT([+|-]\d\d)\:(\d\d)', timezone)
    if offset:
       return convert_using_offset(timestamp, timezone, 'UTC', printtz)
    else:
       return convert_timezone(timestamp, timezone, 'UTC', printtz)