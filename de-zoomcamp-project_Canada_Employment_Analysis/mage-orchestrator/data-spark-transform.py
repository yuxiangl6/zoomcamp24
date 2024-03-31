from pandas import DataFrame
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import col

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    spark = kwargs.get('spark')

    # create spark dataframe
    data_schema = types.StructType([
    types.StructField("ref_date", types.IntegerType(), True),
    types.StructField("geo", types.StringType(), True),
    types.StructField("dguid", types.StringType(), True),
    types.StructField("naics", types.StringType(), True),
    types.StructField("labor_char", types.StringType(), True),
    types.StructField("sex", types.StringType(), True),
    types.StructField("age_group", types.StringType(), True),
    types.StructField("uom", types.StringType(), True),
    types.StructField("uom_id", types.IntegerType(), True),
    types.StructField("scalar_factor", types.StringType(), True),
    types.StructField("scalar_id", types.IntegerType(), True),
    types.StructField("vector", types.StringType(), True),
    types.StructField("coordinate", types.StringType(), True),
    types.StructField("value", types.DoubleType(), True),
    types.StructField("decimal", types.IntegerType(), True)
    ])
    
    spark_df = spark.createDataFrame(data, schema=data_schema)

    # drop column using spark
    df = spark_df.drop('decimal')

    # convert from spark to pandas to faciliate upload
    pd_df = df.toPandas()
    
    return pd_df


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
