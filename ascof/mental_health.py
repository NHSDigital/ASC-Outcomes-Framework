import pandas as pd
import numpy as np
import math
from typing import NamedTuple
#from ascof.mental_health_annex import months_together_input
from ascof import params


months_together_input = params.months_together_input


"""This creates the mental health csv that is appended to the other ascof measures"""



def my_round(n, ndigits=1):
    try:
        part = n * 10**ndigits
        delta = part - int(part)
        # always round "away from 0"
        if delta >= 0.5 or -0.5 < delta <= 0:
            part = math.ceil(part)
        else:
            part = math.floor(part)
        val = part / (10**ndigits)
    except ValueError:
        val = np.nan
    return val


output_path = params.output_path


class LaInput(NamedTuple):
    council: pd.DataFrame
    laref: pd.DataFrame
    region_ref: pd.DataFrame
    type_ref: pd.DataFrame


def import_la_data():
    all_salt_data = params.all_salt_data
    return all_salt_data


def cleaning_months_together(cleaned_df):
    filtered_breakdown = cleaned_df.loc[
        (cleaned_df["BREAKDOWN"] == "CASSR")
        | (cleaned_df["BREAKDOWN"] == "CASSR;Gender")
    ]
    filtered_gender = filtered_breakdown.loc[
        (filtered_breakdown["LEVEL_THREE"] == "MALE")
        | (filtered_breakdown["LEVEL_THREE"] == "FEMALE")
        | (filtered_breakdown["LEVEL_THREE"] == "NONE")
    ]
    filtered_indicator = filtered_gender.loc[
        (filtered_breakdown["METRIC"] == "1H_OUTCOME")
        | (filtered_breakdown["METRIC"] == "1F_OUTCOME")
        | (filtered_breakdown["METRIC"] == "1F_NUMERATOR")
        | (filtered_breakdown["METRIC"] == "1H_NUMERATOR")
    ]
    filtered_indicator["METRIC"] = filtered_indicator["METRIC"].replace(
        {
            "1F_OUTCOME": "1F",
            "1H_OUTCOME": "1H",
        }
    )
    dropped_columns = filtered_indicator.drop(
        columns=[
            "LEVEL_TWO",
            "LEVEL_TWO_DESCRIPTION",
            "STATUS",
            "BREAKDOWN",
        ]
    )
    dropped_columns["LEVEL_THREE"] = dropped_columns["LEVEL_THREE"].replace(
        "NONE", "Total"
    )
    return dropped_columns


def calculate_england(cleaned_ascof_measures):
    Indicators = cleaned_ascof_measures["METRIC"].unique()
    disaggregations = ["MALE", "FEMALE", "Total"]
    all_indicators = pd.DataFrame([])
    for i in Indicators:

        measure_cleaned_ascof_measures = cleaned_ascof_measures[
            cleaned_ascof_measures["METRIC"] == i
        ]
        for d in disaggregations:
            total_cleaned_ascof_measures = measure_cleaned_ascof_measures[
                measure_cleaned_ascof_measures["LEVEL_THREE"] == d
            ]
            total_cleaned_ascof_measures["METRIC_VALUE"] = total_cleaned_ascof_measures[
                "METRIC_VALUE"
            ].mean()
            all_indicators = pd.concat([all_indicators, total_cleaned_ascof_measures])
            all_indicators = all_indicators[["LEVEL_THREE", "METRIC", "METRIC_VALUE"]]
    all_indicators = all_indicators.drop_duplicates()
    all_indicators = pd.melt(
        all_indicators,
        id_vars=["LEVEL_THREE", "METRIC"],
        var_name="Measure Type",
        value_name="Measure Value",
    )
    all_indicators["ASCOF Measure Code"] = (
        all_indicators["METRIC"] + all_indicators["LEVEL_THREE"].str[0]
    )
    mask_1 = all_indicators["ASCOF Measure Code"].str.contains("T")
    all_indicators["ASCOF Measure Code"] = np.where(
        mask_1,
        all_indicators["ASCOF Measure Code"].str.slice(0, 2),
        all_indicators["ASCOF Measure Code"],
    )
    all_indicators["Measure Type"] = "Outcome"
    all_indicators["Geographical Code"] = "England"
    all_indicators["Geographical Description"] = "England"
    all_indicators["Geographical Level"] = "England"
    all_indicators["ONS Code"] = "E92000001"
    all_indicators = all_indicators.rename(
        columns={"METRIC": "Measure Group", "LEVEL_THREE": "Disaggregation"}
    )
    all_indicators = all_indicators[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
            "Measure Value",
            "Measure Group",
        ]
    ]
    return all_indicators


def merge_council_type_region(cleaned_df, la_data):
    merged_type_region = pd.merge(
        cleaned_df,
        la_data.council,
        left_on="LEVEL_ONE",
        right_on="ONS Code",
        how="inner",
    )
    return merged_type_region


def calculate_MH_ASCOF_measures_by_regional_breakdown(cleaned_MH_data):
    regional_breakdown_df_by_name = {}
    for regional_breakdown in ["Council", "Region", "Council Type"]:
        regional_breakdown_df_by_name[regional_breakdown] = cleaned_MH_data.groupby(
            [regional_breakdown, "LEVEL_THREE", "METRIC"]
        )[["METRIC_VALUE"]].mean()
    return regional_breakdown_df_by_name


def clean_council(regional_breakdown_df_by_name, import_data):
    La = regional_breakdown_df_by_name["Council"].reset_index()
    La = pd.melt(
        La,
        id_vars=["Council", "LEVEL_THREE", "METRIC"],
        var_name="Measure Type",
        value_name="Measure Value",
    )
    all_indicators = La
    all_indicators["ASCOF Measure Code"] = (
        all_indicators["METRIC"] + all_indicators["LEVEL_THREE"].str[0]
    )
    mask_1 = all_indicators["ASCOF Measure Code"].str.contains("T")
    all_indicators["ASCOF Measure Code"] = np.where(
        mask_1,
        all_indicators["ASCOF Measure Code"].str.slice(0, 2),
        all_indicators["ASCOF Measure Code"],
    )
    all_indicators["Measure Type"] = "Outcome"
    all_indicators["Geographical Code"] = all_indicators["Council"]
    all_indicators["Geographical Level"] = "Council"
    merge = pd.merge(import_data.laref, all_indicators, on="Council", how="inner")
    x = ["Type", "Region", "RegionName", "CouncilTypeCode", "Council"]
    for a in x:
        del merge[a]

    merge.rename(
        columns={
            "Name": "Geographical Description",
            "Indicator": "Measure Group",
            "METRIC": "Measure Group",
            "LEVEL_THREE": "Disaggregation",
        },
        inplace=True,
    )
    merge = merge[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
            "Measure Value",
            "Measure Group",
        ]
    ]
    return merge


def clean_region(regional_breakdown_df_by_name, import_data):
    region = regional_breakdown_df_by_name["Region"].reset_index()
    region = pd.melt(
        region,
        id_vars=["Region", "LEVEL_THREE", "METRIC"],
        var_name="Measure Type",
        value_name="Measure Value",
    )
    all_indicators = region
    all_indicators["ASCOF Measure Code"] = (
        all_indicators["METRIC"] + all_indicators["LEVEL_THREE"].str[0]
    )
    mask_1 = all_indicators["ASCOF Measure Code"].str.contains("T")
    all_indicators["ASCOF Measure Code"] = np.where(
        mask_1,
        all_indicators["ASCOF Measure Code"].str.slice(0, 2),
        all_indicators["ASCOF Measure Code"],
    )
    all_indicators["Geographical Code"] = all_indicators["Region"]
    all_indicators["Geographical Level"] = "Region"
    all_indicators["Measure Type"] = "Outcome"
    ref = import_data.region_ref.drop_duplicates()
    merge = pd.merge(ref, all_indicators, on="Region", how="left")
    merge.rename(
        columns={"METRIC": "Measure Group", "LEVEL_THREE": "Disaggregation"},
        inplace=True,
    )
    merge = merge[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
            "Measure Value",
            "Measure Group",
        ]
    ]

    return merge


def clean_type(regional_breakdown_df_by_name, import_data):
    c_type = regional_breakdown_df_by_name["Council Type"].reset_index()
    c_type = pd.melt(
        c_type,
        id_vars=["Council Type", "LEVEL_THREE", "METRIC"],
        var_name="Measure Type",
        value_name="Measure Value",
    )
    all_indicators = c_type
    all_indicators["ASCOF Measure Code"] = (
        all_indicators["METRIC"] + all_indicators["LEVEL_THREE"].str[0]
    )
    mask_1 = all_indicators["ASCOF Measure Code"].str.contains("T")
    all_indicators["ASCOF Measure Code"] = np.where(
        mask_1,
        all_indicators["ASCOF Measure Code"].str.slice(0, 2),
        all_indicators["ASCOF Measure Code"],
    )
    all_indicators["Measure Type"] = "Outcome"
    ref = import_data.type_ref.drop_duplicates()
    merge = pd.merge(
        ref,
        all_indicators,
        left_on="Geographical Description",
        right_on="Council Type",
        how="left",
    )
    merge.rename(
        columns={"METRIC": "Measure Group", "LEVEL_THREE": "Disaggregation"},
        inplace=True,
    )
    all_indicators["Geographical Code"] = all_indicators["Council Type"]
    all_indicators["Geographical Level"] = "Council Type"

    merge = merge[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
            "Measure Value",
            "Measure Group",
        ]
    ]

    return merge


def concatenate(council, england, region, c_type):
    concat = pd.concat([council, england, region, c_type])
    # concat.to_csv(output_path+"MH_ascof(python).csv", index=False)
    return concat


def suppress_overall_avg(num_average):
    pivot_with_avg = num_average.pivot_table(
        values="Measure Value",
        index=[
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
        ],
        columns=["Measure Group"],
    )
    pivot_with_avg["suppress_1f_num"] = np.where(
        pivot_with_avg["1F_NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F_NUMERATOR"]
    )
    pivot_with_avg["suppress_1f_oc"] = np.where(
        pivot_with_avg["1F_NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F"]
    )
    pivot_with_avg["suppress_1h_num"] = np.where(
        pivot_with_avg["1H_NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1H_NUMERATOR"]
    )
    pivot_with_avg["suppress_1h_oc"] = np.where(
        pivot_with_avg["1H_NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1H"]
    )
    old = ["1F_NUMERATOR", "1F", "1H_NUMERATOR", "1H"]
    for o in old:
        del pivot_with_avg[o]
    pivot_with_avg.rename(
        columns={
            "suppress_1f_num": "1F_NUMERATOR",
            "suppress_1f_oc": "1F",
            "suppress_1h_num": "1H_NUMERATOR",
            "suppress_1h_oc": "1H",
        },
        inplace=True,
    )
    stacked = pivot_with_avg.stack(level=0).reset_index()
    rename = stacked.rename(columns={0: "Measure value"})
    onef_score = rename.loc[rename["Measure Group"] == "1F"]
    oneh_score = rename.loc[rename["Measure Group"] == "1H"]
    outcome = pd.concat([onef_score, oneh_score])
    outcome = outcome[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "ASCOF Measure Code",
            "Disaggregation",
            "Measure Type",
            "Measure value",
            "Measure Group",
        ]
    ]
    outcome["Measure value"] = outcome["Measure value"].apply(my_round).astype(int)
    outcome["Measure value"] = outcome["Measure value"].replace(99999, "[c]")
    return outcome


def MH_CSV_data_main():
    source_data = import_la_data()
    clean = cleaning_months_together(months_together_input)
    merge = merge_council_type_region(clean, source_data)
    calc = calculate_MH_ASCOF_measures_by_regional_breakdown(merge)
    council = clean_council(calc, source_data)
    england = calculate_england(clean)
    c_type = clean_type(calc, source_data)
    region = clean_region(calc, source_data)
    conc = concatenate(council, england, region, c_type)
    suppress = suppress_overall_avg(conc)
    return suppress



