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

import os
import tempfile
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams["backend"] = "TkAgg"
import numpy
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from dxanalyze.dxgraphs.dxmathmapping import title_mapping
from dxanalyze.dxgraphs.dxmathmapping import label_mapping
from dxanalyze.dxgraphs.dxmathmapping import y_axis_mapping

# plot definition - TODO: review
xlab_fn = ylab_fn = title_fn = 'DejaVu Sans'  
xlab_fs = ylab_fs = 12
title_fs = 14
xlab_fb = ylab_fb = title_fb = 'bold'
xlab_fc = ylab_fc = title_fc = 'k'
leg_loc = 'center left'
lnrblue = '#000080'
lnrred = '#800080'
xtickfs  = ytickfs = 14
xaxis_date_format = '%d-%b-%Y'


def add_trendline(plt, plt_series, label):
    """
    Generate a trend line using a linear regresion
    :param1 plt: Current plot
    :param2 plt_series: plot series
    :param3 label: Name of the series
    """
    x1, y1 = plt_series.index, plt_series
    # generate a linear regresion parameters based on series data
    lin_reg_params = numpy.polyfit(x1, y1, 1)
    # generate a function ax+b based on regresion parameters
    lin_reg_func = numpy.poly1d(lin_reg_params)
    try:
        printlabel = "Linear({})".format(label_mapping[label]["label"])
        printcolor = label_mapping[label]["trendcolor"]
        plt.plot(x1, lin_reg_func(x1), ls='-', color=printcolor, label=printlabel, markersize=2)
    except KeyError as e:
        print("can find entry in label_mapping")
        print(str(e))
        exit(-1)
    


def create_plot(analytic, stat_name, series, y_max_computed, add_trend=False):
    """
    Create a picture and add series
    :param1 analytic: Analytic name to plot
    :param2 stat_name: Statistic name to plot
    :param3 series: dict with series to plot on single graph
    :param4 y_max_computed: max y axis for plot
    :param5 add_trend: flag to add trend line to plot 
    """
    # width/high in inches 
    # it's resized in PPT to 6.5:4
    plt.rcParams['figure.figsize'] = (12, 7.5) 
    plt.figure()
    ax = plt.subplot(1,1,1)
    
    for name, plt_series in series.items():
        plot_series(ax, plt_series, name)
        if add_trend:
            add_trendline(plt, plt_series, name)

    # set Y axis between 0 and y_max_computed
    # set a Y view limit to y_max_computed * 1.2
    # for anything but percent based graphs when view limit is always 100
    if y_max_computed == 0:
        y_max_computed = 1

    ax.set_ybound(0,y_max_computed)
    if analytic == 'cpu' or analytic == 'cpu_summary' or analytic == 'chr':
        plt.ylim(0, y_max_computed)
    else:
        plt.ylim(0, y_max_computed*1.2)
    chrt_details( ax, analytic, stat_name)
  
def plot_series(ax, ser, label):

    try:
        printlabel = label_mapping[label]["label"]
        printcolor = label_mapping[label]["color"]
        printstyle = label_mapping[label]["style"]
    except KeyError as e:
        print("can find entry in label_mapping")
        print(str(e))
        exit(-1)

    if ser.size == 1:
        ser.loc[ser.index[0]+0.0000012] = ser.loc[ser.index[0]]
        printstyle="."

    ser.plot(style=printstyle, color=printcolor, label=printlabel, ax=ax, xlim=(ser.index[0]-0.001, ser.index[-1]+0.001), markersize=1.5)



def chrt_details(ax, analytic, stat_name):
    tempdir = tempfile.gettempdir()
    filename = "{}_{}.png".format(analytic, stat_name)
    xlab = "Date"
    
    try:
        title = title_mapping[analytic][stat_name]
        ylab = y_axis_mapping[stat_name]
    except KeyError as e:
        print("Wrong key to find title or y label")
        print(str(e))
        exit(-1)


    plt.xticks(rotation=60)  # x-axis rotated to 45 deg
    plt.gca().yaxis.grid(True, 'both')  # enable horizontal gridlines
    plt.rc('xtick', labelsize=xtickfs)
    plt.rc('ytick', labelsize=ytickfs)
    ax.xaxis.set_major_formatter(mdates.DateFormatter(xaxis_date_format))
    ax.get_yaxis().get_major_formatter().set_scientific(False)
    ax.set_xlabel(xlab, fontname=xlab_fn, fontsize=xlab_fs, fontweight=xlab_fb, color=xlab_fc)
    ax.set_ylabel(ylab, fontname=ylab_fn, fontsize=ylab_fs, fontweight=ylab_fb, color=ylab_fc)
    ax.set_title(title, fontname=title_fn, fontsize=title_fs, fontweight=title_fb, color=title_fc)
    #removing top and right borders
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # add legend into box
    lgd = ax.legend(loc=leg_loc, bbox_to_anchor=(1, 0.5), frameon=False, fontsize=ytickfs, markerscale=4.)
    imgname = os.path.join(tempdir, filename)
    plt.savefig(imgname, bbox_extra_artists=(lgd, ), bbox_inches='tight')

def create_farmanalyze_chart(df, engine_dict_list,fmindate,fmaxdate):
    
    # Test Data
    #df=pandas.read_csv("C:\Ajay\Delphix\PYDMT\sample_data\pyfarmanalyze\pyfarmanalyze_sample_data.csv")
    #df['max_nt_tx_test'] = round(df['max_nt_tx_test'] / 1024 /1024 , 0 )
    #df['max_nt_rc_test'] = round(df['max_nt_rc_test'] / 1024 /1024 , 0 )
    #df['network'] = round(df['network'] / 1024 /1024 , 0 )
    tempdir = tempfile.gettempdir()
    # create figure and axis objects with subplots()
    fig,ax = plt.subplots()
    fig.set_size_inches(18.5, 10.5, forward=True)

    x = numpy.arange(len(df['engine']))  # the label locations
    width = 0.30  # the width of the bars

    # #rects1 = ax.bar(x - width/2, df['network'], width, label='Network')
    # #rects2 = ax.bar(x + width/2, df['max_nt_test'], width, label='Max Network Test')
    rects1 = ax.bar(x, df['max_nt_rc_test'], width, color="skyblue", label='Max Network Receive Test')
    rects2 = ax.bar(x, df['max_nt_tx_test'], width, color="orange", label='Max Network Transmit Test')
    rects3 = ax.bar(x, df['network'], width, color="red",label='Network Usage')

    for rect1 in rects1:
        height = rect1.get_height()
        txtloc1 = 950 if height > 1000 else height - (height * 0.05)
        if height > 10:
            ax.text(rect1.get_x() + rect1.get_width() / 2.0, txtloc1, '%d' % int(height) + "\n(RC)", ha='center', va='bottom')

    for rect2 in rects2:
        height = rect2.get_height()
        txtloc2 = 850 if height > 1000 else height - (height * 0.15)
        if height > 10:
            ax.text(rect2.get_x() + rect2.get_width() / 2.0, txtloc2, '%d' % int(height) + "\n(TX)", ha='center', va='bottom')

    for rect3 in rects3:
        height = rect3.get_height()
        txtloc3 = 10
        if height > 10:
            ax.text(rect3.get_x() + rect3.get_width() / 2.0, txtloc3, '%d' % int(height) + "\nMBps", ha='center', va='bottom')

    major_yticks = numpy.arange(0, 1001, 100)
    minor_yticks = numpy.arange(0, 1001, 20)
    ax.set_yticks(major_yticks)
    ax.set_yticks(minor_yticks, minor = True)
    ax.set_ylabel("Network Throughput (MBps)",color="orange",fontsize=14, weight='semibold')
    ax.set_ylim(0, 1001)
    ax.yaxis.grid()

    ax.set_title('Period ( {} - {} )'.format(fmindate.strftime("%Y-%m-%d"),fmaxdate.strftime("%Y-%m-%d")),loc='center')
    plt.xticks(x, df['engine'])
    #ax.legend(bbox_to_anchor=(1.05, 4))
    ax.legend(loc='upper right')

    ax2=ax.twinx()

    major_yticks2 = numpy.arange(0, 101, 10)
    minor_yticks2 = numpy.arange(0, 101, 2)
    ax2.set_yticks(major_yticks2)
    ax2.set_yticks(minor_yticks2, minor = True)
    ax2.plot(df.engine, df.cpu,color="blue",marker="o")
    ax2.set_ylabel("CPU (85 %ile)",color="blue",fontsize=14)
    ax2.set_ylim(0, 101)

    fig.tight_layout()
    #plt.show()
    filename = 'pydxfarmanalyze.png'    
    imgname = os.path.join(tempdir, filename)
    #lgd = ax.legend(loc=leg_loc, bbox_to_anchor=(1, 0.5), frameon=False, fontsize=ytickfs, markerscale=4.)
    #fig.savefig('pydxfarmanalyze.jpg', format='png', dpi=200, bbox_inches='tight')
    #fig.savefig(imgname, format='png', bbox_extra_artists=(lgd, ), bbox_inches='tight')
    fig.savefig(imgname, format='png', dpi=200, bbox_inches='tight')
    