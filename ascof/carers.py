

import pandas as pd
import numpy as np
import math
from decimal import Decimal, ROUND_HALF_UP
from ascof import params
from ascof.rounding import my_round5, my_round, my_round3


inputs_folder = params.sace_inputs_folder
outputs_folder = params.output_path

# ASCOF Outputs

table_names = ["1d", "3b", "1i2", "3d2", "3c"]


"""This creates the Carers survey measures (it is not produced as part of the data pipeline due to its bi-annual nature)"""



def load_ascof_measure(table_name):
    sourcedata = pd.read_excel(f"{inputs_folder}ascof_{table_name}.xlsx")
    sourcedata = sourcedata.drop(columns=["AreaName"])
    return sourcedata


def format_area_code(sourcedata):
    sourcedata["AreaCode"] = sourcedata["AreaCode"].str.replace("_x0020_", "")
    sourcedata["AreaCode"] = sourcedata["AreaCode"].str.replace(
        "_x003(\d)_", lambda m: m.group(1), regex=True
    )
    return sourcedata


def suppression(sourcedata):
    sourcedata["Suppress_Gender"] = np.where(
        (sourcedata["Base_male"] < 3) | (sourcedata["Base_female"] < 3), 1, 0
    )
    sourcedata["Suppress_Age"] = np.where(
        (sourcedata["Base_1864"] < 3) | (sourcedata["Base_65OV"] < 3), 1, 0
    )
    sourcedata["Suppress_Total"] = np.where(
        (sourcedata["Base"] < 3) | (sourcedata["Base"] < 3), 1, 0
    )
    sourcedata = sourcedata.fillna(0)
    return sourcedata


def columns_to_be_rounded(sourcedata):
    # select columns to be rounded by 5
    sourcedata_rounded_5 = sourcedata.filter(regex="Denominator|Numerator|Base")
    for column in sourcedata_rounded_5:
        sourcedata_rounded_5[column] = sourcedata_rounded_5[column].apply(my_round5)
    # select columns to be my_rounded
    sourcedata_myround = sourcedata.filter(regex="Outcome|ME")
    for column in sourcedata_myround:
        sourcedata_myround[column] = sourcedata_myround[column].apply(my_round)
    # remove said columns from existing sourced data
    sourcedata_without_rounded_columns = sourcedata.drop(
        sourcedata.filter(regex="Denominator|Numerator|Base|Outcome|ME").columns, axis=1
    )
    # concatebate back
    new_sourcedata = pd.concat(
        [sourcedata_without_rounded_columns, sourcedata_rounded_5, sourcedata_myround],
        axis=1,
    )
    return new_sourcedata


def melt(new_sourcedata):
    melted_sourcedata = new_sourcedata.melt(
        id_vars=["AreaCode", "Suppress_Gender", "Suppress_Age", "Suppress_Total"],
        var_name="Measure_Type_Comb",
        value_name="Value",
    )
    return melted_sourcedata


def measure_type(melted_sourcedata):
    melted_sourcedata["Measure_Type"] = np.where(
        melted_sourcedata["Measure_Type_Comb"].str.contains(pat="Numerator"),
        "Numerator",
        np.where(
            melted_sourcedata["Measure_Type_Comb"].str.contains(pat="ME"),
            "ME",
            np.where(
                melted_sourcedata["Measure_Type_Comb"].str.contains(pat="Outcome"),
                "Outcome",
                np.where(
                    melted_sourcedata["Measure_Type_Comb"].str.contains(pat="Base"),
                    "Base",
                    np.where(
                        melted_sourcedata["Measure_Type_Comb"].str.contains(
                            pat="Denominator"
                        ),
                        "Denominator",
                        "N/A",
                    ),
                ),
            ),
        ),
    )
    return melted_sourcedata


def disaggregation(melted_sourcedata):
    melted_sourcedata["Disaggregation"] = "Total"
    # WARNING - The order is important
    disaggregation_name_by_pattern = {
        "65OV": "65OV",
        "1864": "1864",
        "_fem": "Female",
        "_male": "Male",
    }
    for pattern, disaggregation_name in disaggregation_name_by_pattern.items():
        melted_sourcedata.loc[
            melted_sourcedata["Measure_Type_Comb"].str.contains(pat=pattern),
            "Disaggregation",
        ] = disaggregation_name

    return melted_sourcedata


def consolidate(melted_sourcedata):
    melted_sourcedata["Suppress"] = np.where(
        (melted_sourcedata["Suppress_Gender"] == 1)
        & (melted_sourcedata["Disaggregation"].str.contains("Male|Female"))
        | (melted_sourcedata["Suppress_Age"] == 1)
        & (melted_sourcedata["Disaggregation"].str.contains("1864|65OV"))
        | (melted_sourcedata["Suppress_Total"] == 1)
        & (melted_sourcedata["Disaggregation"].str.contains("Total")),
        1,
        0,
    )
    melted_sourcedata = melted_sourcedata.drop(
        columns=["Suppress_Gender", "Suppress_Age", "Suppress_Total"]
    )
    melted_sourcedata = melted_sourcedata.drop(columns=["Measure_Type_Comb"])
    melted_sourcedata = melted_sourcedata[
        ["AreaCode", "Measure_Type", "Disaggregation", "Value", "Suppress"]
    ]
    return melted_sourcedata


def process_one_table(table_name):
    sourcedata = load_ascof_measure(table_name)
    sourcedata_formattted = format_area_code(sourcedata)
    sup = suppression(sourcedata_formattted)
    rounded = columns_to_be_rounded(sup)
    melted = melt(rounded)
    measures = measure_type(melted)
    disaggregate = disaggregation(measures)
    consolidated = consolidate(disaggregate)
    consolidated.to_csv(outputs_folder + f"{table_name}.csv", index=False)
    return consolidated


def process_all_tables():
    for table_name in table_names:
        processed_table = process_one_table(table_name)
    return processed_table


process_all_tables()

