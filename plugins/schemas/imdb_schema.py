from pyspark.sql.types import (
    ArrayType,
    FloatType,
    MapType,
    StringType,
    StructField,
    StructType,
)

imdb_schema = StructType(
    [
        StructField("title", MapType(StringType(), StringType()), True),
        StructField("cast", MapType(StringType(), ArrayType(StringType())), True),
        StructField("composer", MapType(StringType(), ArrayType(StringType())), True),
        StructField("country", MapType(StringType(), ArrayType(StringType())), True),
        StructField("director", MapType(StringType(), ArrayType(StringType())), True),
        StructField("genre", MapType(StringType(), ArrayType(StringType())), True),
        StructField("kind", MapType(StringType(), StringType()), True),
        StructField("language", MapType(StringType(), ArrayType(StringType())), True),
        StructField("rating", MapType(StringType(), FloatType()), True),
        StructField("runtime", MapType(StringType(), FloatType()), True),
        StructField("vote", MapType(StringType(), FloatType()), True),
        StructField("writer", MapType(StringType(), ArrayType(StringType())), True),
        StructField("year", MapType(StringType(), FloatType()), True),
    ]
)
