#!/usr/local/bin/python
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
# Program Name : pydxanalyze.py
# Description  : Python version of dxanalyze independent of mac/windows/office version
# Author       : Ajay Thotangare
# Created      : 2021-02-15
# Version      : v1.0.0

import os
import logging
from sys import exit

import click

import dxanalyze.dxdata.datafiles as datafiles
import dxanalyze.dxdata.dataprocessing as dataprocessing
import dxanalyze.dxdata.engine as engine
import dxanalyze.dxppt.dxpresentation as dxpresentation
import dxanalyze.dxgraphs.dxmathplot as dxmathplot
from dxanalyze.dxlogging import print_error
from dxanalyze.dxlogging import print_message
from dxanalyze.dxlogging import logging_est

def generate_report(mode, out_location, sync_y, **kwargs):
    """
    Generate a report from engine or offline files
    :param1 mode: oneline or offline processing
    :param2 out_location: output directory location for report
    :param3 sync_y: sync Y across all latency or throughput graphs
    OR
    Generate farmanalyze report on offline files
    param engine_ip: engine ip for online analytics
    param engine_user: engine user for online analytics
    param engine_password: engine password for online analytics
    param start_time: start time for online analytics
    param end_time: end time for online analytics
    param analytic_directory: location of files for offline analytic
    param engine_name: name of the engine (required for offline processing to find file prefix)
    """
    logger = logging.getLogger()
    datafiles.test_dir(out_location)

    if mode == "online":
        engine_ip = kwargs.get('engine_ip')
        engine_user = kwargs.get('engine_user')
        engine_password = kwargs.get('engine_password')
        engine.connect(engine_ip, engine_user, engine_password)
        available_list = engine.get_available_analytics()
        engine_name = engine.get_engine_name()
    elif mode == "offline":
        analytic_directory = kwargs.get('analytic_directory')
        engine_name = kwargs.get('engine_name')
        datafiles.detect_files(analytic_directory, engine_name)
        available_list = datafiles.get_analytics_to_process()
    elif mode == "farmanalyze":
        analytic_directory = kwargs.get('analytic_directory')
        engine_cpufile_mapping,engine_networkfile_mapping,engine_throughputtestfile_mapping = datafiles.detect_farmanalyze_files(analytic_directory)
        farmanalyze_data_list,fmindate,fmaxdate = dataprocessing.generate_farmanalyze_data_summary(engine_cpufile_mapping,engine_networkfile_mapping,engine_throughputtestfile_mapping)
        df =  dataprocessing.create_farmanalyze_df(farmanalyze_data_list)
        dxmathplot.create_farmanalyze_chart(df, farmanalyze_data_list,fmindate,fmaxdate)

    if mode == "farmanalyze":
        dxpresentation.gen_farm_presentation(out_location)
        
    else:
        logger.debug("List of available analytics to process {}".format(str(available_list)))
        analytic_with_data = process_data(mode, available_list, sync_y, **kwargs)
        logger.debug("List of available analytics with data {}".format(str(analytic_with_data)))
        core_required_analytic = set(["cpu", "network", "disk"])
        if core_required_analytic.issubset(set(analytic_with_data)):
            if "nfs" in analytic_with_data or "iscsi" in analytic_with_data:
                dxpresentation.gen_presentation(analytic_with_data, out_location, engine_name)
            else:
                print("NFS or iSCSI data are missing")
        else:
            missing = ",".join(list(core_required_analytic.difference(set(analytic_with_data))))
            print_error("Missing data for {}".format(missing))
            logger.debug("Missing data for {}".format(missing))
            exit(1)
   

def process_data(mode, available_list, sync_y, **kwargs):
    """
    Process data and generate graphs
    :param1 mode: oneline or offline processing
    :param2 available_list: list of analytics to process
    :param3 sync_y: sync Y across all latency or throughput graphs
    :param4 out_location: output directory location for report
    param start_time: start time for online analytics
    param end_time: end time for online analytics
    """  

    io_stats_dataframes_copy = {
        "disk": {},
        "nfs": {},
        "iscsi": {}
    }

    analytic_with_data = []

    logger = logging.getLogger()

    sync_y = True

    for analytic in available_list:
        logger.debug("Processing {} analytic".format(analytic))
        if mode == 'offline':
            analytic_stats = datafiles.process_file(analytic, datafiles.get_files_mapping())
        else:
            start_time = kwargs.get('start_time')
            end_time = kwargs.get('end_time')
            analytic_stats = engine.process_analytics(analytic, start_time, end_time)

        if analytic_stats:
            analytic_with_data.append(analytic)
        
        for stat_dict in analytic_stats:
            logger.debug("Processing statistics for {} analytic".format(analytic))
            
            for stat_name, stat_series in stat_dict.items():
                logger.debug("generate graph for {}".format(stat_name))
                series = {}

                if analytic == 'cpu':  
                    summary = dataprocessing.generate_cpu_summary(stat_series['util']) 
                    if summary:
                        y_max_computed = dataprocessing.get_max_y_axis(analytic + "_summary", stat_name, False)
                        dxmathplot.create_plot(analytic + "_summary", stat_name, summary, y_max_computed, False)
                        

                if analytic == 'network':  
                    summary = dataprocessing.generate_network_summary(stat_series)
                    if summary:
                        y_max_computed = dataprocessing.get_max_y_axis(analytic + "_summary", stat_name, False)
                        dxmathplot.create_plot(analytic + "_summary", stat_name, summary, y_max_computed, False)
                        #exit(1)

                # this loop will generate a dict of all serises from particular graph
                for series_name, dataframe in stat_series.items():
                    logger.debug("Adding series name {} to graph".format(series_name))
                    y_max = dataprocessing.calculate_percentile(0.99, dataframe, series_name) 
                    logger.debug("calculated y_max for processed series is {}".format(y_max))
                    dataprocessing.set_max_y_axis(y_max, analytic, stat_name, sync_y) 
                    if analytic in ["disk", "iscsi", "nfs"] and stat_name == 'throughput':
                        # copy of data frames is needed for Cache Hit Ratio
                        # without deep copy, dataframe has timestamp already tranformed
                        # to a epach by create_serie
                        io_stats_dataframes_copy[analytic][series_name] = dataframe.copy()
                    s = dataprocessing.create_serie(dataframe)
                    series[series_name] = s

                y_max_computed = dataprocessing.get_max_y_axis(analytic, stat_name, sync_y)
                logger.debug("y_max for analytic is {}".format(y_max_computed))
                dxmathplot.create_plot(analytic, stat_name, series, y_max_computed, True)

    dataprocessing.print_cache_hit_ratio(io_stats_dataframes_copy)
    return analytic_with_data


class Config(object):
    def __init__(self):
        self.logfile = "pyanalyze.log"
        self.debug = False
        self.out_directory = None
        self.syncy = False

pass_config = click.make_pass_decorator(Config, ensure=True)


def debug_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(Config)
        state.debug = value
        logging_est(state.logfile, state.debug)
        return value
    return click.option('--debug',
                        is_flag=True,
                        expose_value=False,
                        help='Enables debug mode.',
                        callback=callback)(f)


def logfile_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(Config)
        state.logfile = value
        return value
    return click.option('--logfile',
                        expose_value=False,
                        help='Logfile path and name',
                        callback=callback)(f)


def syncy_option(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(Config)
        state.syncy = value
        return value
    return click.option('--syncy',
                        expose_value=False,
                        is_flag=True,
                        help='Sync Y axis for latency',
                        callback=callback)(f)

def output_directory(f):
    def callback(ctx, param, value):
        state = ctx.ensure_object(Config)
        state.out_directory = value
        return value
    return click.option('--out_directory',
                        expose_value=False,
                        help='Output directory for pyanalyze. Default is a working directory',
                        default="/process",
                        callback=callback)(f)



def common_options(f):
    f = logfile_option(f)
    f = debug_option(f)
    f = output_directory(f)
    f = syncy_option(f)
    return f


@click.group()
@pass_config
def cli(config):
    """
    put help here
    """

@cli.command()
@click.option('--dlpx_engine','-e', default='dlpx_prod_VMAX', prompt='Enter Name/IP of Delphix Engine',
              help='Delphix Engine hostname OR IP Address', required=True)
@click.option('--username','-u', required=True,  prompt='Enter Engine admin username', help="Delphix Engine admin username")
@click.password_option('--password','-p', help='Delphix Admin password to connect delphix engine',
                       required=True, prompt='Enter Engine admin password')
@click.option('--start_time', help="Start time for analytic data. Format YYYY-MM-DD HH24:MI:SS. If not specified a current time minus 7 days will be set")
@click.option('--end_time', help="End time for analytic data. Format YYYY-MM-DD HH24:MI:SS. If not specified a current time will be used")
@common_options
@pass_config
def online(config, dlpx_engine, username, password, start_time, end_time):
    """ 
    This command will generate online mode pydxanalyze report for cpu, network, nfs, iscsi, disk
    It expects a connection details to Delphix Engine.
    """

    generate_report("online", config.out_directory, False, engine_ip=dlpx_engine, engine_user=username, engine_password=password,
                    start_time=start_time, end_time=end_time)



@cli.command()
@click.option('--datadir', default="/process",
              help='Location of directory where dxanalytics data is downloaded')
@click.option('--file_prefix', required=True,
              help='prefix of file (dlpx_engine_name used in dxtools.conf)')
@common_options
@pass_config
def offline(config, datadir, file_prefix):
    """ 
    This command will generate offline mode pydxanalyze report for cpu, network, nfs, iscsi, disk. 
    It expects pre-generated dxanalytics datafiles in datadir location.
    Files can be generated using following command

    For NFS: \n
    dx_get_analytics -d <dlpx_engine> -t cpu,network,nfs,disk -outdir csv
    
    For iSCSI: \n
    dx_get_analytics -d <dlpx_engine> -t cpu,network,iscsi,disk -outdir csv

    For combined engines (iSCSI and NFS): \n
    dx_get_analytics -d <dlpx_engine> -t cpu,network,iscsi,nfs,disk -outdir csv
    """

    generate_report("offline", config.out_directory, config.syncy, analytic_directory=datadir, engine_name=file_prefix)

@cli.command()
@click.option('--datadir', default="/process",
              help='Location of directory where dxanalytics data and throughput test is downloaded')
@common_options
@pass_config
def farmanalyze(config, datadir):
    """ 
    This command will generate offline mode pyfarmanalyze report for cpu and network.
    It expects pre-generated dxanalytics datafiles in datadir location.

    \b
    Following 2 Files from each engine is expected in same folder.
    1) <engine_name>-analytics-cpu-aggregated.csv AND 
    2) <engine_name>-analytics-network-aggregated.csv 

    Files can be generated using following command

    dx_get_analytics -d <dlpx_engine> -t cpu,network -outdir csv

    """

    generate_report("farmanalyze", config.out_directory, config.syncy, analytic_directory=datadir)


if __name__ == "__main__":
    cli()




