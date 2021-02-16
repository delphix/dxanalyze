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

pltblue = '#0066CC'
pltred = '#C14A4E'
pltgrn = '#99cc00'
pltblack = '#000000'
pltdarkgrn = "#339966"

# mapping for a graph title based on analytics and stat_name
title_mapping = {
    "cpu": {
        "utilization": "CPU utilization"
    },
    "cpu_summary": {
        "utilization": "85% CPU Variation for 1 week"
    },
    "nfs": {
        "ops": "NFS Server operations per second",
        "latency": "Internal NFS Latency",
        "throughput": "NFS Throughput from the Delphix Engine to the Targets"
    },
    "iscsi": {
        "ops": "iSCSI Server operations per second",
        "latency": "Internal iSCSI Latency",
        "throughput": "iSCSI Throughput from the Delphix Engine to the Targets"
    },
    "disk": {
        "ops": "Disk IOPS from the Delphix Engine to the Storage",
        "latency": "Internal Disk Latency",
        "throughput": "Disk Throughput from the Delphix Engine to the Storage"
    },
    "network": {
        "throughput": "Total VM Network Throughput from the Delphix Engine to all Sources/Targets"
    },
    "network_summary": {
        "throughput": "85% Network Throughput Variation for 1 week"
    },
    "chr": {
        "chr": "Cache Hit Ratio for Engine"
    }
}

# mapping for Y axis names based on stat_name
y_axis_mapping = {
    "throughput": "Throughput [MB/s]",
    "latency": "Latency [ms]",
    "ops": "Operations per second",
    "utilization": "Utilization",
    "chr": "% Cash Hit"
}


# series names mapping based on column name from CSV
label_mapping = {
    "read_throughput" : {
        "label": "read throughput",
        "color": pltblue,
        "trendcolor": pltblack,
        "style": "."
    },
    "write_throughput" : {
        "label": "write throughput",
        "color": pltred, 
        "trendcolor": pltdarkgrn,
        "style": "."
    },
    "ops_read" : {
        "label": "ops read",
        "color": pltblue, 
        "trendcolor": pltblack,
        "style": "."
    },
    "ops_write" : {
        "label": "ops write",
        "color": pltred, 
        "trendcolor": pltdarkgrn,
        "style": "."
    },
    "read_latency" : {
        "label": "read latency",
        "color": pltblue, 
        "trendcolor": pltblack,
        "style": "."
    },
    "write_latency" : {
        "label": "write latency",
        "color": pltred, 
        "trendcolor": pltdarkgrn,
        "style": "."
    },
    "outBytes" : {
        "label": "read throughput",
        "color": pltblue, 
        "trendcolor": pltblack,
        "style": "."
    },
    "inBytes" : {
        "label": "write throughput",
        "color": pltred, 
        "trendcolor": pltdarkgrn,
        "style": "."
    },
    "util" : {
        "label": "Utilization",
        "color": pltred, 
        "trendcolor": pltblack,
        "style": "."
    },
    "min" : {
        "label": "utilization_min",
        "color": pltblue, 
        "style": "-"
    },
    "max" : {
        "label": "utilization_max",
        "color": pltred, 
        "style": "-"
    },
    "85percentile" : {
        "label": "utilization_85pct",
        "color": pltgrn, 
        "style": "-"
    },
    "inBytes85pct" : {
        "label": "85 pct write [MB/s]",
        "color": pltred, 
        "style": "-"
    },
    "outBytes85pct" : {
        "label": "85 pct read [MB/s]",
        "color": pltblue, 
        "style": "-"
    },
    "chr" : {
        "label": "Cache Hit Ratio",
        "color": pltblue, 
        "style": "."
    }
}

