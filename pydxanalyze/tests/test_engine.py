import pandas
import json
from os.path import join
from pandas.util.testing import assert_frame_equal
from pandas.util.testing import assert_almost_equal
from unittest import TestCase
from unittest import main
from unittest.mock import patch
from unittest import mock
from dxanalyze.dxdata.engine import process_io
from dxanalyze.dxdata.engine import generate_pages
from dxanalyze.dxdata.engine import process_analytics
import dxanalyze.dxdata.engine as engine
from delphixpy.v1_8_0.web.analytics import analytics
from delphixpy.v1_8_0.web.objects.DatapointSet import DatapointSet 
from delphixpy.v1_8_0.web.objects.DatapointStream import DatapointStream 


def analytic_mock(a, b, **kwargs):
    f = open(join("tests","nfs.json"))
    jsondata = json.load(f)
    f.close()
    s = DatapointSet()
    s = s.from_dict(jsondata["result"])
    return s

class Test_datafile(TestCase):
    @patch('dxanalyze.dxdata.engine.__engine_time_zone', "Europe/Dublin")
    def test_process_io(self):
        nfsio = {
            "#timestamp" : [ "2019-07-22 14:55:00", "2019-07-22 14:56:00", "2019-07-22 14:57:00", "2019-07-22 14:58:00",
                             "2019-07-22 14:59:00", "2019-07-22 15:00:00", "2019-07-22 15:01:00", "2019-07-22 15:02:00",
                             "2019-07-22 15:03:00", "2019-07-22 15:04:00", "2019-07-22 15:05:00", "2019-07-22 15:05:00",
                             "2019-07-22 15:05:00", "2019-07-22 15:05:00", "2019-07-22 15:06:00", "2019-07-22 15:07:00",
                             "2019-07-22 15:08:00", "2019-07-22 15:09:00", "2019-07-22 15:10:00", "2019-07-22 15:10:00",
                             "2019-07-22 15:10:00", "2019-07-22 15:10:00", "2019-07-22 15:11:00", "2019-07-22 15:12:00",
                             "2019-07-22 15:13:00",
             ],
            "read_throughput":  [ 0.041926,0.032812,0.280256,0.037630,0.038280,0.103254,0.036198,0.035286,0.043098,
                                  0.035156,0.038411,0.038411,0.001041,0.001041,0.033854,0.035156,0.035156,0.034895,
                                  0.038671,0.038671,0.002083,0.002083,0.032812,0.033854,0.039973],
            "read_latency": [ 0.02,0.02,0.05,0.02,0.03,0.02,0.02,0.02,0.02,0.02,0.02,0.02,
                              0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02]
        }

        engine.__engine_time_zone = "Europe/Dublin"
        f = open(join("tests","nfs.json"))
        jsondata = json.load(f)
        f.close()

        df = pandas.DataFrame(nfsio)
        csvlikepanda = process_io(jsondata["result"]["datapointStreams"])
        assert_almost_equal(df, csvlikepanda[["#timestamp","read_latency","read_throughput"]], check_less_precise=True )

    @patch('dxanalyze.dxdata.engine.__engine_time_zone', "Europe/Dublin")
    def test_generate_pages(self):
        page_list = []
        for i in generate_pages("2019-01-01 00:00:00", "2019-01-01 12:00:00"):
            page_list.append(i)

        result_list = [('2019-01-01 00:00:00', '2019-01-01 01:00:00'), ('2019-01-01 01:00:00', '2019-01-01 02:00:00'), 
                       ('2019-01-01 02:00:00', '2019-01-01 03:00:00'), ('2019-01-01 03:00:00', '2019-01-01 04:00:00'), 
                       ('2019-01-01 04:00:00', '2019-01-01 05:00:00'), ('2019-01-01 05:00:00', '2019-01-01 06:00:00'), 
                       ('2019-01-01 06:00:00', '2019-01-01 07:00:00'), ('2019-01-01 07:00:00', '2019-01-01 08:00:00'), 
                       ('2019-01-01 08:00:00', '2019-01-01 09:00:00'), ('2019-01-01 09:00:00', '2019-01-01 10:00:00'), 
                       ('2019-01-01 10:00:00', '2019-01-01 11:00:00'), ('2019-01-01 11:00:00', '2019-01-01 12:00:00')]

        self.assertListEqual(page_list, result_list)

    @patch('dxanalyze.dxdata.engine.__engine_time_zone', "Europe/Dublin")
    @mock.patch.object(
        analytics, 'get_data', new=analytic_mock
    )
    def test_process_analytics(self):
        nfsio = {
            "#timestamp" : [ "2019-07-22 14:55:00", "2019-07-22 14:56:00", "2019-07-22 14:57:00", "2019-07-22 14:58:00",
                             "2019-07-22 14:59:00", "2019-07-22 15:00:00", "2019-07-22 15:01:00", "2019-07-22 15:02:00",
                             "2019-07-22 15:03:00", "2019-07-22 15:04:00", "2019-07-22 15:05:00", "2019-07-22 15:05:00",
                             "2019-07-22 15:05:00", "2019-07-22 15:05:00", "2019-07-22 15:06:00", "2019-07-22 15:07:00",
                             "2019-07-22 15:08:00", "2019-07-22 15:09:00", "2019-07-22 15:10:00", "2019-07-22 15:10:00",
                             "2019-07-22 15:10:00", "2019-07-22 15:10:00", "2019-07-22 15:11:00", "2019-07-22 15:12:00",
                             "2019-07-22 15:13:00",
             ],
            "read_throughput":  [ 0.041926,0.032812,0.280256,0.037630,0.038280,0.103254,0.036198,0.035286,0.043098,
                                  0.035156,0.038411,0.038411,0.001041,0.001041,0.033854,0.035156,0.035156,0.034895,
                                  0.038671,0.038671,0.002083,0.002083,0.032812,0.033854,0.039973],
            "read_latency": [ 0.02,0.02,0.05,0.02,0.03,0.02,0.02,0.02,0.02,0.02,0.02,0.02,
                              0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02,0.02]
        }

        df = pandas.DataFrame(nfsio)
        stats_list = process_analytics("nfs", "2019-07-22 14:55:00", "2019-07-22 15:13:00")
        for stat in stats_list:
            if "throughput" in stat:
                result_df = stat["throughput"]["read_throughput"]
                assert_almost_equal(df[["#timestamp","read_throughput"]], result_df, check_less_precise=True )
            if "latency" in stat:
                result_df = stat["latency"]["read_latency"]
                assert_almost_equal(df[["#timestamp","read_latency"]], result_df, check_less_precise=True )

if __name__ == '__main__':
    main()