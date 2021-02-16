import pandas
from os.path import join
from unittest import TestCase
from unittest import main
from dxanalyze.dxdata.datafiles import process_file
from dxanalyze.dxdata.datafiles import create_dataframes
from dxanalyze.dxdata.datafiles import files_mapping
from dxanalyze.dxdata.datafiles import detect_files
from pandas.util.testing import assert_frame_equal

class Test_datafile(TestCase):
    def test_detect_files(self):
        detect_files("tests", "test")
        self.assertDictEqual(files_mapping, {"cpu": join("tests","test-analytics-cpu-raw.csv"), "disk": join("tests","test-analytics-disk-raw.csv")})

    def test_process_file(self):
        files_mapping = {"cpu": join("tests","test-analytics-cpu-raw.csv")}
        stat = process_file("cpu", files_mapping)
        self.assertListEqual(list(stat[0].keys()), ["utilization"])

    def test_create_dataframes(self):
        csvdata = pandas.read_csv(join("tests","test-analytics-cpu-raw.csv"))
        datadict = {
            "#timestamp" : [ "2019-03-20 11:55:00", "2019-03-20 11:56:00", "2019-03-20 11:57:00",
                             "2019-03-20 11:58:00", "2019-03-20 11:59:00", "2019-03-20 12:00:00" ],
            "util": [ 25.81, 26.29, 24.89, 25.57, 34.68, 49.87]
        }
        foo = pandas.DataFrame(data=datadict)
        cpustat = create_dataframes('cpu', csvdata)
        assert_frame_equal(cpustat[0]["utilization"]["util"], foo)

if __name__ == '__main__':
    main()



