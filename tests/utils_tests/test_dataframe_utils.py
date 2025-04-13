import pytest
import utils.dataframe_utils as utils
from helpers import df_to_list


class TestDataframeUtils:
    @pytest.fixture(autouse=True)
    def _setUp(self, spark):
        self.spark = spark

    def test_convert_to_int(self):
        data = [
            {"value": 1.0},
            {"value": None},
            {"value": 3.0},
            {"value": 4.1},
            {"value": 4.5},
            {"value": 4.9},
            {"value": 5.0},
        ]
        df = self.spark.createDataFrame(data)
        df_converted = utils.convert_to_int(df, "value")
        assert df_to_list(df_converted) == [
            {"value": 1},
            {"value": None},
            {"value": 3},
            {"value": 4},
            {"value": 4},
            {"value": 4},
            {"value": 5},
        ]

    def test_explode_key(self):
        data = [
            {
                "value": {
                    "1": "a",
                    "2": "b",
                    "3": "c",
                }
            },
        ]
        df = self.spark.createDataFrame(data)
        df_exploded = utils.explode_key(df, "value")
        assert df_to_list(df_exploded) == [
            {"id": "1", "value": "a"},
            {"id": "2", "value": "b"},
            {"id": "3", "value": "c"},
        ]

    def test_merge_dfs(self):
        data = [
            {"id": "1", "value1": "a"},
            {"id": "2", "value1": "b"},
            {"id": "3", "value1": "c"},
        ]
        df1 = self.spark.createDataFrame(data)

        data = [
            {"id": "1", "value2": "a"},
            {"id": "2", "value2": "b"},
            {"id": "3", "value2": "c"},
        ]
        df2 = self.spark.createDataFrame(data)

        df_merged = utils.merge_dfs([df1, df2], key="id")
        assert df_to_list(df_merged) == [
            {"id": "1", "value1": "a", "value2": "a"},
            {"id": "2", "value1": "b", "value2": "b"},
            {"id": "3", "value1": "c", "value2": "c"},
        ]
