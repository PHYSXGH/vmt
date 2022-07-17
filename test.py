import pytest

from main import CalculateDailyTransactions


def test_pipeline():
    input = ["2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99", \
             "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95", \
             "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22", \
             "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030", \
             "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08", \
             "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12"]

    # use the CalculateDailyTransactions function in the test, as this contains all the logic. We're not testing
    # the file IO, as there is no point in doing that
    output = input | CalculateDailyTransactions()

    # no transactions pre-2010 and the sum on 2017-03-18 is 2102.22 which means values under 20 are ignored
    expected = ['2017-03-18,2102.22', '2017-08-31,13700000023.08', '2018-02-27,129.12']

    assert output == expected
