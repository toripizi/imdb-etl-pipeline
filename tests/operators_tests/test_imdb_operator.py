import pytest
from airflow import DAG
from factories.imdb.imdb_processed import ImdbProcessedFactory
from helpers import DEFAULT_START_DATE, assert_df_schema, df_to_list
from operators.imdb_operator import ImdbOperator
from schemas.imdb_schema import imdb_schema


class TestImdbOperator:
    @pytest.fixture(autouse=True)
    def _setUp(self, spark, mocker):
        self.spark = spark
        self.dag = DAG(
            "TestImdbOperator",
            default_args={"start_date": DEFAULT_START_DATE},
        )
        self.imdb_operator = ImdbOperator(
            dag=self.dag,
            task_id="test_imdb_operator",
        )
        self.mock_spark_session(mocker)
        self.mock_write_to_parquet(mocker)
        self.mock_setup_azure_base_path(mocker)

    def mock_spark_session(self, mocker):
        def mock(context):
            self.imdb_operator.spark = self.spark

        mocker.patch.object(
            self.imdb_operator,
            "setup_spark_session",
            side_effect=mock,
        )

    def mock_load_imdb_data(self, mocker, data):
        mocker.patch.object(
            self.imdb_operator,
            "load_imdb_data",
            return_value=self.spark.createDataFrame(data, imdb_schema),
        )

    def mock_write_to_parquet(self, mocker):
        def mock(df):
            self.imdb_operator.resulf_df = df

        mocker.patch.object(
            self.imdb_operator,
            "write_to_parquet",
            side_effect=mock,
        )

    def mock_setup_azure_base_path(self, mocker):
        mocker.patch.object(self.imdb_operator, "setup_azure_base_path")

    def test_execute(self, mocker):
        data = [ImdbProcessedFactory(num_movies=3)]
        self.mock_load_imdb_data(mocker, data)

        self.imdb_operator.execute(None)
        assert self.imdb_operator.resulf_df.count() == 3
        df = self.imdb_operator.resulf_df.orderBy("id").limit(1)
        assert df_to_list(df) == [
            {
                "id": "0",
                "title": "Movie Title 0",
                "year": 2000,
                "vote": 1000,
                "runtime": 90,
                "rating": 7.0,
                "kind": "movie",
                "cast": ["Actor 0_1", "Actor 0_2", "Actor 0_3"],
                "director": ["Director 0"],
                "writer": ["Writer 0_1", "Writer 0_2"],
                "genre": ["Drama", "Thriller"],
                "language": ["English", "Spanish"],
                "country": ["USA", "UK"],
                "composer": ["Composer 0"],
            }
        ]
        expected_schema = """
            |-- id: string (nullable = false)
            |-- cast: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- composer: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- country: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- director: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- genre: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- kind: string (nullable = true)
            |-- language: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- rating: float (nullable = true)
            |-- runtime: integer (nullable = true)
            |-- title: string (nullable = true)
            |-- vote: integer (nullable = true)
            |-- writer: array (nullable = true)
            |    |-- element: string (containsNull = true)
            |-- year: integer (nullable = true)
        """
        assert_df_schema(df, expected_schema)

    def test_execute_with_duplicates(self, mocker):
        data = [
            ImdbProcessedFactory(num_movies=3),
            ImdbProcessedFactory(num_movies=3),
        ]
        self.mock_load_imdb_data(mocker, data)

        self.imdb_operator.execute(None)
        assert self.imdb_operator.resulf_df.count() == 3
        df = self.imdb_operator.resulf_df.orderBy("id").limit(1)
        assert df_to_list(df) == [
            {
                "id": "0",
                "title": "Movie Title 0",
                "year": 2000,
                "vote": 1000,
                "runtime": 90,
                "rating": 7.0,
                "kind": "movie",
                "cast": ["Actor 0_1", "Actor 0_2", "Actor 0_3"],
                "director": ["Director 0"],
                "writer": ["Writer 0_1", "Writer 0_2"],
                "genre": ["Drama", "Thriller"],
                "language": ["English", "Spanish"],
                "country": ["USA", "UK"],
                "composer": ["Composer 0"],
            }
        ]

    def test_execute_with_null_values(self, mocker):
        data = [
            ImdbProcessedFactory(num_movies=3, title=None, year=None),
        ]
        self.mock_load_imdb_data(mocker, data)

        self.imdb_operator.execute(None)
        assert self.imdb_operator.resulf_df.count() == 0
