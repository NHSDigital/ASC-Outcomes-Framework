from os import rename
import pandas as pd
import numpy as np
from typing import NamedTuple
from ascof.rounding import my_round5, my_round
from ascof import  params

months_together_input = params.months_together_input


"""This produces all the 1f and 1H mental health scores, alongside its denominator, numerator and gender breakdowns"""

def import_la_data():

    all_salt_data = params.all_salt_data
    return all_salt_data


def cleaning_months_together(cleaned_df):
    filtered_breakdown = cleaned_df.loc[
        (cleaned_df["BREAKDOWN"] == "CASSR")
        | (cleaned_df["BREAKDOWN"] == "CASSR;Gender")
        | (cleaned_df["BREAKDOWN"] == "England;Gender")
        | (cleaned_df["BREAKDOWN"] == "England")
    ]
    filtered_gender = filtered_breakdown.loc[
        (filtered_breakdown["LEVEL_THREE"] == "MALE")
        | (filtered_breakdown["LEVEL_THREE"] == "FEMALE")
        | (filtered_breakdown["LEVEL_THREE"] == "NONE")
    ]
    filtered_gender["METRIC"] = filtered_gender["METRIC"].replace(
        {
            "1F_OUTCOME": "1F SCORE",
            "1H_OUTCOME": "1H SCORE",
            "1F_NUMERATOR": "1F NUMERATOR",
            "1H_NUMERATOR": "1H NUMERATOR",
        }
    )
    dropped_columns = filtered_gender.drop(
        columns=[
            "LEVEL_TWO",
            "LEVEL_TWO_DESCRIPTION",
            "STATUS",
            "BREAKDOWN",
        ]
    )
    dropped_columns["LEVEL_THREE"] = dropped_columns["LEVEL_THREE"].replace(
        "NONE", "All"
    )
    dropped_columns.rename(
        columns={
            "LEVEL_ONE": "ONS Code",
            "LEVEL_THREE": "Gender",
            "LEVEL_ONE_DESCRIPTION": "CASSR",
            "REPORTING_PERIOD": "Month",
        },
        inplace=True,
    )

    return dropped_columns


def merge_council_type_region(cleaned_df, la_data):
    merged_type_region = pd.merge(
        cleaned_df,
        la_data.council,
        on="ONS Code",
        how="inner",
    )
    merged_type_region["CASSR Code"] = merged_type_region["Council"]
    drop_column = merged_type_region.drop(columns="ONS Code")
    return drop_column


def filter_num_denom(merged_data):
    filtered_metric = merged_data.loc[
        (merged_data["METRIC"] == "1F NUMERATOR")
        | (merged_data["METRIC"] == "1H NUMERATOR")
        | (merged_data["METRIC"] == "DENOMINATOR")
    ]
    return filtered_metric


def filter_score(merged_data):
    filtered_metric = merged_data.loc[
        (merged_data["METRIC"] == "1F SCORE") | (merged_data["METRIC"] == "1H SCORE")
    ]
    return filtered_metric


def calculate_MH_score_ASCOF_measures_by_regional_breakdown(cleaned_MH_data):
    regional_breakdown_df_by_name = {}
    for regional_breakdown in ["Region", "Council Type"]:
        regional_breakdown_df_by_name[regional_breakdown] = cleaned_MH_data.groupby(
            [regional_breakdown, "Month", "Gender", "METRIC"]
        )[["METRIC_VALUE"]].mean()
    return regional_breakdown_df_by_name


def calculate_MH_denom_num_measures_by_regional_breakdown(cleaned_MH_data):
    regional_breakdown_df_by_name = {}
    for regional_breakdown in ["Region", "Council Type"]:
        regional_breakdown_df_by_name[regional_breakdown] = cleaned_MH_data.groupby(
            [regional_breakdown, "Month", "Gender", "METRIC"]
        )[["METRIC_VALUE"]].sum()
    return regional_breakdown_df_by_name


def clean_region(regional_breakdown_df_by_name, import_data):
    region = regional_breakdown_df_by_name["Region"].reset_index()
    region = pd.melt(
        region,
        id_vars=["Region", "Month", "Gender", "METRIC"],
        var_name="Measure Type",
        value_name="METRIC_VALUE",
    )
    all_indicators = region

    ref = import_data.region_ref.drop_duplicates()
    merge = pd.merge(ref, all_indicators, on="Region", how="left")
    merge.rename(columns={"Geographical Description": "CASSR"}, inplace=True)
    merge["CASSR Code"] = merge["Region"]
    drop_columns = merge.drop(columns=["Region", "Measure Type"])
    drop_columns = drop_columns[
        [
            "CASSR Code",
            "Month",
            "CASSR",
            "ONS Code",
            "Gender",
            "METRIC",
            "METRIC_VALUE",
        ]
    ]
    return drop_columns


def clean_type(regional_breakdown_df_by_name, import_data):
    c_type = regional_breakdown_df_by_name["Council Type"].reset_index()
    c_type = pd.melt(
        c_type,
        id_vars=["Council Type", "Month", "Gender", "METRIC"],
        var_name="Measure Type",
        value_name="METRIC_VALUE",
    )
    all_indicators = c_type
    ref = import_data.type_ref.drop_duplicates()
    merge = pd.merge(
        ref,
        all_indicators,
        left_on="Geographical Description",
        right_on="Council Type",
        how="left",
    )
    merge.rename(columns={"Geographical Description": "CASSR"}, inplace=True)
    merge["CASSR Code"] = merge["Geographical Code"]
    merge = merge[
        [
            "CASSR Code",
            "Month",
            "CASSR",
            "ONS Code",
            "Gender",
            "METRIC",
            "METRIC_VALUE",
        ]
    ]
    return merge


def clean_council(cleaned_months, import_data):
    all_indicators = pd.merge(
        import_data.laref,
        cleaned_months,
        left_on="Council",
        right_on="CASSR Code",
        how="left",
    )
    all_indicators = all_indicators[
        ["CASSR Code", "Month", "CASSR", "ONS Code", "Gender", "METRIC", "METRIC_VALUE"]
    ]
    return all_indicators


def clean_england(merged_data):
    filtered_eng = merged_data[merged_data["CASSR"] == "England"]
    filtered_eng["CASSR Code"] = "England"
    filtered_eng["ONS Code"] = "E92000001"
    filtered_eng = filtered_eng[
        ["CASSR Code", "Month", "CASSR", "ONS Code", "Gender", "METRIC", "METRIC_VALUE"]
    ]
    return filtered_eng


def concat(type_1, type_2, region_1, region_2, council, england):
    concat = pd.concat([type_1, type_2, region_1, region_2, council, england])
    return concat


def average_numerator(cleaned_df):
    all_data = pd.DataFrame([])
    unique_council = cleaned_df["CASSR"].unique()
    for i in unique_council:
        council = cleaned_df[cleaned_df["CASSR"] == i]
        score = ["1F NUMERATOR", "1H NUMERATOR"]
        for s in score:
            metric = council[council["METRIC"] == s]
            gender = ["MALE", "FEMALE", "All"]
            for g in gender:
                metric_gendered = metric[metric["Gender"] == g]
                metric_gendered["AVERAGE VALUE"] = metric_gendered[
                    "METRIC_VALUE"
                ].mean()
                del metric_gendered["Month"]
                metric_gendered["Month"] = "Num_Average"
                del metric_gendered["METRIC_VALUE"]
                metric_gendered.rename(
                    columns={"AVERAGE VALUE": "METRIC_VALUE"}, inplace=True
                )
                metric_gendered
                all_data = pd.concat([all_data, metric_gendered])
    super_cleaned_data = pd.concat([cleaned_df, all_data])
    super_cleaned_data = super_cleaned_data.loc[
        super_cleaned_data["Month"] == "Num_Average"
    ]
    return super_cleaned_data.drop_duplicates()


def average_outcome(cleaned_df):
    all_data = pd.DataFrame([])
    unique_council = cleaned_df["CASSR"].unique()
    for i in unique_council:
        council = cleaned_df[cleaned_df["CASSR"] == i]
        score = ["1F SCORE", "1H SCORE"]
        for s in score:
            metric = council[council["METRIC"] == s]
            gender = ["MALE", "FEMALE", "All"]
            for g in gender:
                metric_gendered = metric[metric["Gender"] == g]
                metric_gendered["AVERAGE VALUE"] = metric_gendered[
                    "METRIC_VALUE"
                ].mean()
                del metric_gendered["Month"]
                metric_gendered["Month"] = "Average"
                del metric_gendered["METRIC_VALUE"]
                metric_gendered.rename(
                    columns={"AVERAGE VALUE": "METRIC_VALUE"}, inplace=True
                )
                metric_gendered
                all_data = pd.concat([all_data, metric_gendered])
                all_data
    super_cleaned_data = pd.concat([cleaned_df, all_data])
    super_cleaned_data = super_cleaned_data.loc[
        super_cleaned_data["Month"] == "Average"
    ]
    return super_cleaned_data.drop_duplicates()


def suppress_overall_avg(oc_avg, num_average):
    avg_all_data = pd.concat([oc_avg, num_average])
    avg_all_data_num = avg_all_data.loc[avg_all_data["Month"] == "Average"]
    pivot_with_avg_oc = avg_all_data.loc[avg_all_data["Month"] == "Num_Average"]
    all_avg = pd.concat([avg_all_data_num, pivot_with_avg_oc])
    pivot_with_avg = all_avg.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR Code", "CASSR", "ONS Code", "Gender"],
        columns=["METRIC", "Month"],
    )
    pivot_with_avg["suppress_1f_num"] = np.where(
        pivot_with_avg["1F NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F NUMERATOR"]
    )
    pivot_with_avg["suppress_1f_oc"] = np.where(
        pivot_with_avg["1F NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F SCORE"]
    )
    pivot_with_avg["suppress_1h_num"] = np.where(
        pivot_with_avg["1H NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1H NUMERATOR"]
    )
    pivot_with_avg["suppress_1h_oc"] = np.where(
        pivot_with_avg["1H NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1H SCORE"]
    )
    old = ["1F NUMERATOR", "1F SCORE", "1H NUMERATOR", "1H SCORE"]
    for o in old:
        del pivot_with_avg[o]
    pivot_with_avg.rename(
        columns={
            "suppress_1f_num": "1F NUMERATOR",
            "suppress_1f_oc": "1F SCORE",
            "suppress_1h_num": "1H NUMERATOR",
            "suppress_1h_oc": "1H SCORE",
        },
        inplace=True,
    )
    stacked = pivot_with_avg.stack(level=0).reset_index()
    rename = stacked.rename(columns={"": "METRIC_VALUE"})
    onef_score = rename.loc[rename["METRIC"] == "1F SCORE"]
    oneh_score = rename.loc[rename["METRIC"] == "1H SCORE"]
    outcome = pd.concat([onef_score, oneh_score])
    outcome["Month"] = "Average"
    return outcome


def suppressed_num_denom_oc(averaged_data):
    pivot_with_avg = averaged_data.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR Code", "CASSR", "ONS Code", "Month", "Gender"],
        columns="METRIC",
    )

    pivot_without_avg = pivot_with_avg.iloc[
        pivot_with_avg.index.get_level_values("Month") != "Average"
    ]
    pivot_with_avg = pivot_with_avg.iloc[
        pivot_with_avg.index.get_level_values("Month") == "Average"
    ]
    pivot_without_avg["suppress_1f_num"] = np.where(
        pivot_without_avg["1F NUMERATOR"] < 5, 99999, pivot_without_avg["1F NUMERATOR"]
    )
    pivot_without_avg["suppress_1f_oc"] = np.where(
        pivot_without_avg["suppress_1f_num"] == 99999,
        99999,
        pivot_without_avg["1F SCORE"],
    )
    pivot_without_avg["suppress_1h_num"] = np.where(
        pivot_without_avg["1H NUMERATOR"] < 5, 99999, pivot_without_avg["1H NUMERATOR"]
    )
    pivot_without_avg["suppress_1h_oc"] = np.where(
        pivot_without_avg["suppress_1h_num"] == 99999,
        99999,
        pivot_without_avg["1H SCORE"],
    )
    pivot_without_avg["suppress_denom"] = np.where(
        pivot_without_avg["DENOMINATOR"] < 5, 99999, pivot_without_avg["DENOMINATOR"]
    )
    old = ["1F NUMERATOR", "1F SCORE", "1H NUMERATOR", "1H SCORE", "DENOMINATOR"]
    for o in old:
        del pivot_without_avg[o]
    pivot_without_avg.rename(
        columns={
            "suppress_1f_num": "1F NUMERATOR",
            "suppress_1f_oc": "1F SCORE",
            "suppress_1h_num": "1H NUMERATOR",
            "suppress_1h_oc": "1H SCORE",
            "suppress_denom": "DENOMINATOR",
        },
        inplace=True,
    )
    all_data = pd.concat([pivot_without_avg, pivot_with_avg])
    stacked = all_data.stack(level=0).reset_index()
    rename = stacked.rename(columns={0: "METRIC_VALUE"})
    return rename


def applying_round(cleaned_df):
    score = ["1F SCORE", "1H SCORE"]
    filtered_score = cleaned_df[cleaned_df["METRIC"].isin(score)]
    filtered_num_denom = cleaned_df[~cleaned_df["METRIC"].isin(score)]
    filtered_score["METRIC_VALUE"] = filtered_score["METRIC_VALUE"].apply(my_round)
    filtered_num_denom["METRIC_VALUE"] = filtered_num_denom["METRIC_VALUE"].apply(
        my_round5
    )
    all_data = pd.concat([filtered_score, filtered_num_denom])
    return all_data


def pivotted_MH(super_clean):
    pivot = super_clean.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR", "ONS Code", "Month", "Gender"],
        columns="METRIC",
    )
    return pivot


def replace(super_clean):
    super_clean = super_clean.replace([99999, 100000], "[c]")
    return super_clean


def onef_oneh_cassr_main():
    source_data = import_la_data()
    clean = cleaning_months_together(months_together_input)
    merge = merge_council_type_region(clean, source_data)
    filtered_num_denom = filter_num_denom(merge)
    filtered_score = filter_score(merge)
    calc_1 = calculate_MH_score_ASCOF_measures_by_regional_breakdown(filtered_score)
    calc_2 = calculate_MH_denom_num_measures_by_regional_breakdown(filtered_num_denom)
    type_01 = clean_type(calc_1, source_data)
    type_02 = clean_type(calc_2, source_data)
    region_01 = clean_region(calc_1, source_data)
    region_02 = clean_region(calc_2, source_data)
    council = clean_council(merge, source_data)
    england = clean_england(clean)
    conc = concat(type_01, type_02, region_01, region_02, council, england)
    average_oc = average_outcome(conc)
    average_num = average_numerator(conc)
    suppress_num_denom = suppressed_num_denom_oc(conc)
    suppress_overall_oc = suppress_overall_avg(average_oc, average_num)
    merged_suppression = pd.concat([suppress_num_denom, suppress_overall_oc])
    rounded = applying_round(merged_suppression)
    pivot = pivotted_MH(rounded)
    final = replace(pivot)
    final.fillna("[z]")
    final.to_csv(params.output_path+"MH_1F_1H_CASSR.csv")
    return final




