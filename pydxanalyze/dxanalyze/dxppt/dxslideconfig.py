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

slide_with_pictures = {
    "cpu": {
        5: "cpu_summary_utilization.png",
        9: "cpu_utilization.png"
    },
    "network": {
        6: "network_summary_throughput.png",
        13: "network_throughput.png"
    },
    "disk": {
        12: "disk_throughput.png",
        16: "disk_latency.png",
        19: "disk_ops.png"
    },
    "nfs": {
        10: "nfs_throughput.png",
        14: "nfs_latency.png",
        17: "nfs_ops.png",
    },
    "iscsi": {
        11: "iscsi_throughput.png",
        15: "iscsi_latency.png",
        18: "iscsi_ops.png"
    },
    "chr": {
        20: "chr_chr.png"
    },
    "farm": {
        2: "pydxfarmanalyze.png"
    }    
}

report_template = 'pydxanalyze-template.pptx'
farm_report_template = 'pydxfarmanalyze-template.pptx'
skip_title_update_slides = [2, 23, 24, 25]