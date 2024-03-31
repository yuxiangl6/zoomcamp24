import io
import pandas as pd
import requests
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    canada_labour_url = "https://github.com/thefabscientist/Waterloo-DS3-Group-1-Project/blob/main/CanadaLabourData.csv.gz?raw=true"

    # canada_labour_dtypes = {
    #                 'REF_DATE': pd.Int64Dtype(),
    #                 'GEO': str,
    #                 'DGUID': str,
    #                 'Labour force characteristics': str,
    #                 'North American Industry Classification System (NAICS)': str,
    #                 'Sex': str,
    #                 'Age group': str,
    #                 'UOM': str,
    #                 'UOM_ID': pd.Int64Dtype(),
    #                 'SCALAR_FACTOR': str,
    #                 'SCALAR_ID': pd.Int64Dtype(),
    #                 'VECTOR': str,
    #                 'COORDINATE': str,
    #                 'value': float,
    #                 'STATUS': str,
    #                 'SYMBOL': str,
    #                 'TERMINATED': str,
    #                 'DECIMALS': pd.Int64Dtype()
    # }
    
    # read data
    data = pd.read_csv(canada_labour_url,sep=",",compression="gzip")

    # change column names
    data = data.rename(columns={"REF_DATE": "ref_date", 
                                "GEO": "geo",
                                "DGUID": "dguid",
                                "North American Industry Classification System (NAICS)": "naics",
                                "Labour force characteristics": "labor_char", 
                                "Sex": "sex",
                                "Age group": "age_group",
                                "UOM": "uom",
                                "UOM_ID": "uom_id",
                                "SCALAR_FACTOR": "scalar_factor",
                                "SCALAR_ID": "scalar_id",
                                "VECTOR": "vector",
                                "COORDINATE": "coordinate",
                                "VALUE": "value",
                                "STATUS": "status",
                                "SYMBOL": "symbol",
                                "TERMINATED": "terminated",
                                "DECIMALS": "decimals"
                                })

    # filtered data using pandas in this step first because dataset is huge (400MB) for codespace (max 32GB) to handle
    data = data.drop(columns=['status', 'symbol', 'terminated', 'terminated'])
    data = data[(data['age_group'] != '15 to 24 years') & (data['age_group'] != '15 years and over') & 
                (data['ref_date'] == 2019)]

    return data


# @test
# def test_output(output, *args) -> None:
#     """
#     Template code for testing the output of the block.
#     """
#     assert output is not None, 'The output is undefined'
