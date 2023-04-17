
import pandas as pd
from ascof import params

"""This creates the HES (Hospital Episode Statistics) output alone. It is not part of the data pipeline flow but is useful for producing the HES figures in isolation"""


def import_data():
    hes_data = params.hes_data
    hes_data.columns = hes_data.iloc[0]
    hes_data = hes_data.iloc[4:]
    hes_data = hes_data.replace(to_replace="*", value=0)
    return hes_data


def clean_hes_data(hes_data):   
    exempts =  [
        1001,
        "E",
        "EM",
        "I",
        "IL",
        "M",
        "NE",
        "NW",
        "O",
        "OS",
        "SW",
        "U",
        "D",
        "WM",
        "YH",
        "L",
        "S",
        "SE"]
    
    filtered = hes_data[~hes_data['Lookup'].isin(exempts)]
    return filtered 


def sum_columns_for_agebands(numeric_clean_hes_data):
    numeric_clean_hes_data["65-74"] = (
        numeric_clean_hes_data["65 to 74 Male"]
        + numeric_clean_hes_data["65 to 74 Female"]
    )
    numeric_clean_hes_data["75-84"] = (
        numeric_clean_hes_data["75 to 84 Male"]
        + numeric_clean_hes_data["75 to 84 Female"]
    )
    numeric_clean_hes_data["85OV"] = (
        numeric_clean_hes_data["85 and over Male"]
        + numeric_clean_hes_data["85 and over Female"]
    )
    
    numeric_clean_hes_data["TOTAL"] = numeric_clean_hes_data["TOTAL 65 and over Male"] + numeric_clean_hes_data["TOTAL 65 and over Female"]
    return numeric_clean_hes_data


def formatting(numeric_clean_hes_data):
    formatted_hes_data = numeric_clean_hes_data.rename(columns={"Lookup": "Lacode", 
    "TOTAL 65 and over Male": "MALE", "TOTAL 65 and over Female" : "FEMALE" })
    formatted_hes_data = formatted_hes_data[["Lacode", "65-74", "75-84", 
    "85OV", "MALE", "FEMALE", "TOTAL"]]
    return formatted_hes_data

def melt(formatted_hes_data):
    melted_HES_assets = formatted_hes_data.melt(
        id_vars=["Lacode"], var_name="Disaggregation", value_name="Denominator"
    )
    melted_HES_assets['Indicator'] = "2B2"
    return melted_HES_assets

def main():
    importing = import_data()
    cleaning =  clean_hes_data(importing)
    age_bands = sum_columns_for_agebands(cleaning)
    formats = formatting(age_bands)
    melted = melt(formats)
    return melted

