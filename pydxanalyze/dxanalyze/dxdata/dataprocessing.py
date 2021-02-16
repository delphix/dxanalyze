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

from datetime import datetime
from datetime import timedelta

from pandas import pandas
from sys import exit

from matplotlib.dates import date2num

# from dxanalyze.dxdata.datafiles import detect_files
# from dxanalyze.dxdata.datafiles import process_file
# import dxanalyze.dxdata.datafiles as datafiles
from dxanalyze.dxgraphs.dxmathplot import create_plot

iocolumns = set(["#timestamp","read_throughput","write_throughput","ops_read","ops_write" \
                 ,"read_latency","write_latency"])

y_axis_max = {
    "global": {
        "throughput": 0,
        "latency": 0
    }
}

def create_dataframes(analytic_name, csvdata):
    """
    Function is processing Pandas dataframe and split it into list of data frames
    :param1 analytic_name: name of the analytic to process
    :param2 files_mapping: dict with files to analytic mapping
    Return a list of dict { stat: { series :Pandas Dataframe}}. Each stat will be one graph

    Ex. return list 
    [ 
      {
        "throughput" : { 
            "read_throupugh" : DataFrame, 
            "write_throughput" : DataFrame} 
      },
      {
        "latency" : { 
            "read_latency" : DataFrame, 
            "write_latency" : DataFrame} 
      },
    ]
    """

    stat_list = []
    if analytic_name == 'cpu':
        # check if dataframe has two required columns
        if "#timestamp" in csvdata.columns and "util" in csvdata.columns:
            stat_dict = {}
            stat_dict["util"] = csvdata[["#timestamp", "util"]]
            stat_list.append({"utilization": stat_dict})

    if analytic_name == 'network':
        # check if dataframe has all required columns
        if "#timestamp" in csvdata.columns and "inBytes" in csvdata.columns \
           and "outBytes" in csvdata.columns:
            stat_dict = {}
            csvdata.loc[:, "inBytes"] = csvdata.loc[:, "inBytes"]/1024/1024
            csvdata.loc[:, "outBytes"] = csvdata.loc[:, "outBytes"]/1024/1024
            stat_dict["inBytes"] = csvdata[["#timestamp", "inBytes"]]
            stat_dict["outBytes"] = csvdata[["#timestamp", "outBytes"]]
            stat_list.append({"throughput": stat_dict})

    if analytic_name in ['disk','nfs','iscsi']:
        # check if dataframe has all required columns

        if iocolumns.issubset(csvdata.columns):
            stat_dict = {}
            stat_dict["read_throughput"] = csvdata[["#timestamp", "read_throughput"]]
            stat_dict["write_throughput"] = csvdata[["#timestamp", "write_throughput"]]
            stat_list.append({"throughput": stat_dict})
            stat_dict = {}
            stat_dict["ops_read"] = csvdata[["#timestamp", "ops_read"]]
            stat_dict["ops_write"] = csvdata[["#timestamp", "ops_write"]]
            stat_list.append({"ops": stat_dict})
            stat_dict = {}
            stat_dict["read_latency"] = csvdata[["#timestamp", "read_latency"]]
            stat_dict["write_latency"] = csvdata[["#timestamp", "write_latency"]]
            stat_list.append({"latency": stat_dict})

    return stat_list


def calculate_percentile(percentile, dataframe, column_name):
    """
    Calculate a percentile for dataframe column
    :param1 percentile: percentile to calculate
    :param2 dataframe: dataframe to process
    :param3 column_name: column name to calculate percentile on
    Return a percentile rounded to 2 digits
    """
    return round(dataframe[column_name].quantile(percentile),2 )


def get_max_y_axis(analitycs, stat_name, sync_y):
    """
    Get a max y_axis value
    :param1 analitycs: analytic name [ cpu, disk, nfs, iscsi, network]
    :param2 stat_name: statistic name [ throughput, latency, ops]
    :param3 sync_y: if true synchronize a y axis across all IO related analytics [ disk, nfs, iscsi]
    Return a y axis value
    """
    if analitycs == 'cpu' or analitycs == 'cpu_summary':
        return 100

    if sync_y and stat_name !='ops':
        return y_axis_max["global"][stat_name]
    else:
        return y_axis_max[analitycs][stat_name]


def set_max_y_axis(y_axis, analitycs, stat_name, sync_y):
    """
    Set a max y_axis value. 
    With sync_y set to False, If y_axis already exist for stat_name and it's less than a new one,
    it will be updated with a new value
    With sync_y set to True, y_axis is compared across all analytics for which a throughput or latency
    are calculated

    OPS statistics are always local. There is no point to compare NFS ops with disk IOPS on same scale

    :param1 analitycs: analytic name [ cpu, disk, nfs, iscsi, network]
    :param2 stat_name: statistic name [ throughput, latency, ops]
    :param3 sync_y: if true synchronize a y axis across all IO related analytics [ disk, nfs, iscsi]
    Return a y axis value
    """
    if analitycs != 'cpu':
        if sync_y and stat_name !='ops':
            if y_axis_max["global"][stat_name] < y_axis:
                y_axis_max["global"][stat_name] = y_axis
        else:
            if analitycs not in y_axis_max:
                y_axis_max[analitycs] = {}

            if stat_name in y_axis_max[analitycs]:
                if y_axis_max[analitycs][stat_name] < y_axis:
                    y_axis_max[analitycs][stat_name] = y_axis
            else:
                y_axis_max[analitycs][stat_name] = y_axis



def create_serie(df):
    """
    Create a Pandas serie used by matplotlib to print series on graph 
    :param1 df: Pandas dataframs with 2 columns - 1st if #timestamp, 2nd is a value to print  
    Return a Pandas serie
    """  
    df["#timestamp"] = date2num(pandas.to_datetime(df["#timestamp"],format='%Y-%m-%d %H:%M:%S'))
    sr = pandas.Series(df[df.columns[1]].values, index=df["#timestamp"])
    return sr


def convert_to_frame(input_serie):
    """
    Convert Pandas serie to Pandas dataframe and remove index
    Plus fix a timestamp after grouping by operation to have date plus time value
    :param1 input_serie: Pandas serie
    Return a Pandas dataframe
    """  
    input_serie = input_serie.to_frame()
    input_serie = input_serie.reset_index(level=["#timestamp"])
    input_serie["#timestamp"] = input_serie["#timestamp"] + " 00:00:00"
    return input_serie



def generate_cpu_summary(df):
    """
    Generate a CPU summary based on DataFrame
    It will calculate min, max and 85 pct by grouping dataframe on timestamp column
    for date only ( 10 characters )
    Return a dict of series for min, max and 85 percentile 
    """  
    series = {
        "min": None,
        "max": None,
        "85percentile": None
    }

    ser = df.groupby([df.loc[: ,"#timestamp"].str[:10]])['util'].min()
    series["min"] = create_serie(convert_to_frame(ser))

    ser = df.groupby([df.loc[: ,"#timestamp"].str[:10]])['util'].max()
    series["max"] = create_serie(convert_to_frame(ser))

    ser = df.groupby([df.loc[: ,"#timestamp"].str[:10]])['util'].quantile(.85)
    series["85percentile"] = create_serie(convert_to_frame(ser))

    return series

def generate_network_summary(stat_series):
    """
    Generate a network summary based on dictonary of data frames 
    It will calculate 85 pct by grouping dataframe on timestamp column
    for date only ( 10 characters ) for each series ( inBytes and outBytes)
    Return a dict of series for 85 percentile for inBytes and outBytes
    """  
    series = {}

    for series_name, dataframe in stat_series.items():
        if not dataframe.empty:
            y_max = calculate_percentile(1, dataframe, series_name)
            set_max_y_axis(y_max, "network_summary", "throughput", False) 
            ser = dataframe.groupby([dataframe.loc[: ,"#timestamp"].str[:10]])[series_name].quantile(.85)
            series[series_name + "85pct"] = create_serie(convert_to_frame(ser))

    return series


def generate_cache_hit_ratio(io_stats_dataframes):
    """
    Generate a cache hit ratio based on all IO dataframes 
    :param1 io_stats_dataframes: dict with all IO stats with reads
    return a DataFrame with cache hit ratio for each timestamp 
    or empty dataframe if there is no cache hit ratio calculated
    """  

    if "read_throughput" in io_stats_dataframes["disk"]:
        df_disk = io_stats_dataframes["disk"]["read_throughput"]
    else:
        return pandas.DataFrame(columns = ['#timestamp', 'cachehit'])
        
    if "read_throughput" in io_stats_dataframes["iscsi"]:
        df_iscsi = io_stats_dataframes["iscsi"]["read_throughput"]
    else:
        df_iscsi = pandas.DataFrame(columns = ['#timestamp', 'read_throughput'])
    
    if "read_throughput" in io_stats_dataframes["nfs"]:
        df_nfs = io_stats_dataframes["nfs"]["read_throughput"]
    else:
        df_nfs = pandas.DataFrame(columns = ['#timestamp', 'read_throughput'])

    # merge NFS and iSCSI using a outer join so any of the data frame can be empty
    # NaN will be filled by zeros 
    df_nfs_iscsi = pandas.merge(df_nfs, df_iscsi, on="#timestamp", how="outer", suffixes=("_nfs","_iscsi"))
    df_nfs_iscsi = df_nfs_iscsi.fillna(0)

    # merge disk with NFS/iSCSI using a inner join, so only rows with data from both frames will be joined
    cache_hit_ratio_df = pandas.merge(df_disk, df_nfs_iscsi, on="#timestamp", how="inner")
    # calculate a cache hit ratio and add as a new column
    cache_hit_ratio_df["cachehit"] = 100 - ((cache_hit_ratio_df['read_throughput'] * 100) / (cache_hit_ratio_df['read_throughput_nfs'] + cache_hit_ratio_df['read_throughput_iscsi'] ))

    # DataFrame copy is required to avoid SettingWithCopyWarning when chaging a timestamp in create_serie procedure
    cache_hit_ratio_df2 = cache_hit_ratio_df[["#timestamp", "cachehit"]].copy(deep=True)

    return cache_hit_ratio_df2


def print_cache_hit_ratio(io_stats_dataframes):
    dataframe = generate_cache_hit_ratio(io_stats_dataframes)
    if not dataframe.empty:
        s = create_serie(dataframe)
        create_plot("chr", "chr", { "chr": s }, 100)


def generate_farmanalyze_data_summary(engine_cpufile_mapping,engine_networkfile_mapping,engine_throughputtestfile_mapping):
    engine_dict_list = []
    isntdictempty = not bool(engine_throughputtestfile_mapping)
    if not isntdictempty:
        dft = pandas.read_csv(engine_throughputtestfile_mapping['all'])
        dft = dft.groupby(['#engine','direction'], sort=False)['throughput'].max().reset_index()

    fmindate = None
    fmaxdate = None
    for keyC, valueC in engine_cpufile_mapping.items():
        engine_dict = {}
        engine_dict['engine'] = keyC
        dfc=pandas.read_csv(valueC)
        maxcpu=round(dfc['utilization_85pct'].max(),0)
        mindt=datetime.strptime(dfc['#time'].min(), '%Y-%m-%d')
        maxdt=datetime.strptime(dfc['#time'].max(), '%Y-%m-%d')
        if fmindate is None:
            fmindate = mindt
        if fmindate > mindt:
            fmindate = mindt
        if fmaxdate is None:
            fmaxdate = maxdt
        if fmaxdate < maxdt:
            fmaxdate = maxdt
        engine_dict['cpu'] = maxcpu
        for keyN, valueN in engine_networkfile_mapping.items():
            if keyC == keyN:
                dfn=pandas.read_csv(valueN)
                maxnetwork=(dfn['inBytes_85pct'] + dfn['outBytes_85pct']).max()
                maxnetworkMB = round(maxnetwork / 1024 /1024 , 0 )
                mindt=datetime.strptime(dfn['#time'].min(), '%Y-%m-%d')
                maxdt=datetime.strptime(dfn['#time'].max(), '%Y-%m-%d')
                if fmindate is None:
                    fmindate = mindt
                if fmindate > mindt:
                    fmindate = mindt
                if fmaxdate is None:
                    fmaxdate = maxdt
                if fmaxdate < maxdt:
                    fmaxdate = maxdt
                engine_dict['network'] = maxnetworkMB


        dftt = pandas.DataFrame()
        dftr = pandas.DataFrame()

        if not isntdictempty:
            transmitdf = dft.loc[(dft['#engine'] == keyC) & (dft['direction'] == 'TRANSMIT')]
            if transmitdf.empty == False:
                engine_dict['max_nt_tx_test'] = transmitdf.iloc[0]['throughput']
                engine_dict['max_nt_tx_test'] = ( engine_dict['max_nt_tx_test'] / 8 )

            receivedf = dft.loc[(dft['#engine'] == keyC) & (dft['direction'] == 'RECEIVE')]
            if receivedf.empty == False:
                engine_dict['max_nt_rc_test'] = receivedf.iloc[0]['throughput']
                engine_dict['max_nt_rc_test'] = ( engine_dict['max_nt_rc_test'] / 8 )

        engine_dict_list.append(engine_dict)
    return engine_dict_list,fmindate,fmaxdate

def create_farmanalyze_df(engine_dict_list):
    df = pandas.DataFrame(engine_dict_list, columns =['engine', 'cpu', 'network','max_nt_tx_test','max_nt_rc_test'], dtype = float)
    return df