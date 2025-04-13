import pytest
from helpers import create_testing_pyspark_session


@pytest.fixture(scope="session")
def spark():
    spark = create_testing_pyspark_session()
    yield spark
    spark.stop()
