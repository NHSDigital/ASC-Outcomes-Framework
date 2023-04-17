import pandas as pd
from ascof import params

"""This creates the  ONS denominators used by the SALT measures"""


def import_data():
    ASCOF_2A_DENOM = params.ASCOF_2A_DENOM
    return ASCOF_2A_DENOM


def clean_ons_data(ASCOF_2A_DENOM):
    ASCOF_2A_DENOMINATOR = ASCOF_2A_DENOM.drop(
        [
            "Name",
            "Geography",
            "18+",
            "65-74",
            "75-84",
            "85+",
            "Total adult population",
            "Prop 18+",
            "Prop 18-64",
            "Prop 65-74",
            "Prop75-84",
            "Prop_of_65",
            "Prop 85+",
        ],
        axis=1,
    )
    TWO_A_DENOMINATOR = ASCOF_2A_DENOMINATOR.rename(
        columns={"DH Code": "Lacode", "18-64": "2A1", "65+": "2A2"}
    )
    TWO_A_DENOMINATOR["Lacode"] = TWO_A_DENOMINATOR["Lacode"].astype(str)

    return TWO_A_DENOMINATOR


def melt_ons_data(TWO_A_DENOMINATOR):
    melted_ONS_data = TWO_A_DENOMINATOR.melt(
        id_vars=["Lacode"], var_name="Indicator", value_name="Denominator"
    )
    return melted_ONS_data


def main():
    importing = import_data()
    cleaning = clean_ons_data(importing)
    melting = melt_ons_data(cleaning)
    return melting



