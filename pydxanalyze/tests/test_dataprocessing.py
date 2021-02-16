import pandas
from pandas.util.testing import assert_frame_equal
from unittest import TestCase
from unittest import main
from dxanalyze.dxdata.dataprocessing import calculate_percentile
from dxanalyze.dxdata.dataprocessing import set_max_y_axis
from dxanalyze.dxdata.dataprocessing import get_max_y_axis
from dxanalyze.dxdata.dataprocessing import generate_cache_hit_ratio
from dxanalyze.dxdata.dataprocessing import generate_cpu_summary
from dxanalyze.dxdata.dataprocessing import generate_network_summary
from dxanalyze.dxdata.dataprocessing import create_dataframes



class Test_datafile(TestCase):
    def test_calculate_percentile(self):
        csvdata = pandas.read_csv("tests/test-analytics-disk-raw.csv")
        df = csvdata[["#timestamp","read_throughput"]]
        pct = calculate_percentile(0.95, df, "read_throughput")
        self.assertEqual(pct, 42.87)


    def test_create_dataframes_cpu(self):
        datadict = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-21 11:58:00", "2019-03-21 11:59:00", "2019-03-21 12:00:00" ],
            "util": [ 25.81, 26.29, 24.89, 25.57, 34.68, 49.87]
        }


        df = pandas.DataFrame(datadict)
        df = pandas.DataFrame(datadict)
        series_list = create_dataframes('cpu', df)
        assert_frame_equal(series_list[0]["utilization"]["util"], df)


    def test_create_dataframes_nfs(self):

        nfsio = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "read_throughput":  [ 20, 80, 80, 90, 45, 10],
            "write_throughput": [ 2, 8, 8, 9, 4, 1],
            "ops_read": [1000, 2000, 3000, 1000, 2000, 3000],
            "ops_write": [100, 200, 300, 100, 200, 300],
            "read_latency": [2, 3, 4, 4, 3, 2],
            "write_latency": [1, 3, 6, 6, 3, 1]
        }

        df = pandas.DataFrame(nfsio)
        series_list = create_dataframes('nfs', df)
        for s in series_list:
            if "throughput" in s:
                assert_frame_equal(s["throughput"]["read_throughput"], df[["#timestamp", "read_throughput"]])
                assert_frame_equal(s["throughput"]["write_throughput"], df[["#timestamp", "write_throughput"]])
            if "latency" in s:
                assert_frame_equal(s["latency"]["read_latency"], df[["#timestamp", "read_latency"]])
                assert_frame_equal(s["latency"]["write_latency"], df[["#timestamp", "write_latency"]])
            if "ops" in s:
                assert_frame_equal(s["ops"]["ops_read"], df[["#timestamp", "ops_read"]])
                assert_frame_equal(s["ops"]["ops_write"], df[["#timestamp", "ops_write"]])

    def test_generate_cpu_summary(self):
        datadict = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-21 11:58:00", "2019-03-21 11:59:00", "2019-03-21 12:00:00" ],
            "util": [ 25.81, 26.29, 24.89, 25.57, 34.68, 49.87]
        }

        result_dict = {
            "min": {
                "#timestamp": [737138.0, 737139.0],
                "min": [24.89, 25.57]
            },
            "max": {
                "#timestamp": [737138.0, 737139.0],
                "max": [26.29, 49.87]
            },
            "85percentile": {
                "#timestamp": [737138.0, 737139.0],
                "85percentile": [26.146, 45.313]
            },
        }

        df = pandas.DataFrame(datadict)
        series_dict = generate_cpu_summary(df)

        for s in ["min", "max", "85percentile"]:
            series = series_dict[s].to_frame()
            series = series.reset_index()
            series = series.rename(columns={0:s})
            df_min = pandas.DataFrame(result_dict[s])
            assert_frame_equal(df_min, series)

    def test_generate_network_summary(self):
        inbytes = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-21 11:58:00", "2019-03-21 11:59:00", "2019-03-21 12:00:00" ],
            "inBytes": [ 10, 20, 30, 60, 40, 20]
        }

        outbytes = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-21 11:58:00", "2019-03-21 11:59:00", "2019-03-21 12:00:00" ],
            "outBytes": [ 15, 25, 35, 65, 45, 25]
        }

        indf = pandas.DataFrame(inbytes)
        outdf = pandas.DataFrame(outbytes)

        inDict = {
            "inBytes": indf,
            "outBytes": outdf
        }


        result_dict = {
            "inBytes85pct": {
                "#timestamp": [737138.0, 737139.0],
                "85pct": [27.0, 54.0]
            },
            "outBytes85pct": {
                "#timestamp": [737138.0, 737139.0],
                "85pct": [32.0, 59.0]
            }
        }


        series_dict = generate_network_summary(inDict)
        for s in ["inBytes85pct", "outBytes85pct"]:
            series = series_dict[s].to_frame()
            series = series.reset_index()
            series = series.rename(columns={0:"85pct"})
            df_min = pandas.DataFrame(result_dict[s])
            assert_frame_equal(df_min, series)

    def test_generate_network_summary_nodata(self):

        indf = pandas.DataFrame()
        outdf = pandas.DataFrame()

        inDict = {
            "inBytes": indf,
            "outBytes": outdf
        }


        series_dict = generate_network_summary(inDict)
        self.assertDictEqual(series_dict, {})
 

    def test_generate_cache_hit_ratio(self):

        diskio = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "read_throughput": [ 10, 20, 0, 90, 30, 5.25]
        }

        nfsio = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "read_throughput": [ 20, 80, 80, 90, 45, 10]
        }

        cache_hit_result = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "cachehit": [ 50, 75, 100, 0, 33.33333333333333, 47.5]
        }


        diskdf = pandas.DataFrame(diskio)
        nfsdf = pandas.DataFrame(nfsio)
        cache_hit_result = pandas.DataFrame(cache_hit_result)

        iodf = {
            "disk": {
                "read_throughput": diskdf
            },
            "nfs": {
                "read_throughput": nfsdf
            },
            "iscsi": {}
        }

        cache_ratio_df = generate_cache_hit_ratio(iodf)
        
        assert_frame_equal(cache_ratio_df, cache_hit_result)


    def test_generate_cache_hit_ratio_disk_only(self):
        diskio = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "read_throughput": [ 10, 20, 0, 90, 30, 5.25]
        }


        diskdf = pandas.DataFrame(diskio)

        iodf = {
            "disk": {
                "read_throughput": diskdf
            },
            "nfs": {},
            "iscsi": {}
        }

        cache_ratio_df = generate_cache_hit_ratio(iodf)
        self.assertEqual(cache_ratio_df.empty, True)


    def test_generate_cache_hit_ratio_nodisk(self):
        nfsio = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "read_throughput": [ 10, 20, 0, 90, 30, 5.25]
        }


        nfsdf = pandas.DataFrame(nfsio)

        iodf = {
            "disk": {},
            "nfs": {
                "read_throughput": nfsdf
            },
            "iscsi": {}
        }

        cache_ratio_df = generate_cache_hit_ratio(iodf)
        self.assertEqual(cache_ratio_df.empty, True)



    def test_generate_cache_hit_ratio_nostats(self):
        iodf = {
            "disk": {},
            "nfs": {},
            "iscsi": {}
        }

        cache_ratio_df = generate_cache_hit_ratio(iodf)
        self.assertEqual(cache_ratio_df.empty, True)

    def test_setting_y_axis_local_1(self):
        # setting 2 for same stat - like read and write 
        set_max_y_axis(10, "nfs", "latency", False)
        set_max_y_axis(20, "nfs", "latency", False)
        # output should be 20 - always higher
        y_axis = get_max_y_axis("nfs","latency", False)
        self.assertEqual(20, y_axis)

    def test_setting_y_axis_local_2(self):
        # setting 2 for same stat - like read and write 
        set_max_y_axis(10, "nfs", "latency", False)
        set_max_y_axis(20, "nfs", "latency", False)
        # setting 2 for same stat - like read and write 
        set_max_y_axis(100, "nfs", "throughput", False)
        set_max_y_axis(200, "nfs", "throughput", False)
        # output should be 20 - always higher
        y_axis = get_max_y_axis("nfs","latency", False)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("nfs","throughput", False)
        self.assertEqual(200, y_axis)

    def test_setting_y_axis_local_3(self):
        # setting 2 for same stat - like read and write 
        set_max_y_axis(10, "nfs", "latency", False)
        set_max_y_axis(20, "nfs", "latency", False)
        set_max_y_axis(1, "disk", "latency", False)
        set_max_y_axis(2, "disk", "latency", False)
        # setting 2 for same stat - like read and write 
        set_max_y_axis(100, "nfs", "throughput", False)
        set_max_y_axis(200, "nfs", "throughput", False)
        set_max_y_axis(1000, "disk", "throughput", False)
        set_max_y_axis(2000, "disk", "throughput", False)
        set_max_y_axis(1500, "network", "throughput", False)
        set_max_y_axis(2500, "network", "throughput", False)
        # output should be 20 - always higher
        y_axis = get_max_y_axis("nfs","latency", False)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("nfs","throughput", False)
        self.assertEqual(200, y_axis)
        y_axis = get_max_y_axis("disk","latency", False)
        self.assertEqual(2, y_axis)
        y_axis = get_max_y_axis("disk","throughput", False)
        self.assertEqual(2000, y_axis)
        y_axis = get_max_y_axis("network","throughput", False)
        self.assertEqual(2500, y_axis)

    def test_setting_y_axis_global(self):
        # setting 2 for same stat - like read and write 
        set_max_y_axis(10, "nfs", "latency", True)
        set_max_y_axis(20, "nfs", "latency", True)
        set_max_y_axis(1, "disk", "latency", True)
        set_max_y_axis(2, "disk", "latency", True)
        # setting 2 for same stat - like read and write 
        set_max_y_axis(100, "nfs", "throughput", True)
        set_max_y_axis(200, "nfs", "throughput", True)
        set_max_y_axis(1000, "disk", "throughput", True)
        set_max_y_axis(2000, "disk", "throughput", True)
        # output should be 20 - always higher
        y_axis = get_max_y_axis("nfs","latency", True)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("nfs","throughput", True)
        self.assertEqual(2000, y_axis)
        y_axis = get_max_y_axis("disk","latency", True)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("disk","throughput", True)
        self.assertEqual(2000, y_axis)

    def test_setting_y_axis_global_2(self):
        # setting 2 for same stat - like read and write 
        set_max_y_axis(10, "nfs", "latency", True)
        set_max_y_axis(20, "nfs", "latency", True)
        set_max_y_axis(1, "disk", "latency", True)
        set_max_y_axis(2, "disk", "latency", True)
        # setting 2 for same stat - like read and write 
        set_max_y_axis(100, "nfs", "throughput", True)
        set_max_y_axis(200, "nfs", "throughput", True)
        set_max_y_axis(1000, "disk", "throughput", True)
        set_max_y_axis(2000, "disk", "throughput", True)
        set_max_y_axis(1000, "network", "throughput", True)
        set_max_y_axis(2500, "network", "throughput", True)
        # output should be 20 - always higher
        y_axis = get_max_y_axis("nfs","latency", True)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("nfs","throughput", True)
        self.assertEqual(2500, y_axis)
        y_axis = get_max_y_axis("disk","latency", True)
        self.assertEqual(20, y_axis)
        y_axis = get_max_y_axis("disk","throughput", True)
        self.assertEqual(2500, y_axis)
        y_axis = get_max_y_axis("network","throughput", True)
        self.assertEqual(2500, y_axis)

if __name__ == '__main__':
    main()