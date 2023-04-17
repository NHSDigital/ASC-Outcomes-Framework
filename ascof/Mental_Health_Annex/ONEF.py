import pandas as pd
import numpy as np
from ascof.Mental_Health_Annex.ONEF_and_ONEH import (
    import_la_data,
    cleaning_months_together,
    months_together_input,
    concat,
    clean_region,
    clean_type,
    clean_council,
    clean_england,
    calculate_MH_score_ASCOF_measures_by_regional_breakdown,
    calculate_MH_denom_num_measures_by_regional_breakdown,
    merge_council_type_region,
    filter_num_denom,
    filter_score,
    applying_round,
    replace,
)


"""This creates only the 1F aggregations for the full year in review, alongside with its average score as well as its gender breakdowns per month."""

def clean_indicator_onef(cleaned_df):
    filtered_indicator = cleaned_df.loc[
        (cleaned_df["METRIC"] == "1F NUMERATOR")
        | (cleaned_df["METRIC"] == "1F SCORE")
        | (cleaned_df["METRIC"] == "DENOMINATOR")
    ]
    return filtered_indicator


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


def average_outcome_indicator(filtered_df):
    all_data = pd.DataFrame([])
    unique_council = filtered_df["CASSR"].unique()
    for i in unique_council:
        council = filtered_df[filtered_df["CASSR"] == i]
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
                metric_gendered["Month"] = " Average"
                del metric_gendered["METRIC_VALUE"]
                metric_gendered.rename(
                    columns={"AVERAGE VALUE": "METRIC_VALUE"}, inplace=True
                )
                metric_gendered
                all_data = pd.concat([all_data, metric_gendered])
                all_data
    super_cleaned_data = pd.concat([all_data, filtered_df])
    super_cleaned_data = super_cleaned_data.loc[
        super_cleaned_data["Month"] == " Average"
    ]
    return super_cleaned_data.drop_duplicates()


def suppressed_num_denom_oc(averaged_data):
    pivot_with_avg = averaged_data.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR Code", "CASSR", "ONS Code", "Month", "Gender"],
        columns="METRIC",
    )

    pivot_without_avg = pivot_with_avg.iloc[
        pivot_with_avg.index.get_level_values("Month") != " Average"
    ]
    pivot_with_avg = pivot_with_avg.iloc[
        pivot_with_avg.index.get_level_values("Month") == " Average"
    ]
    pivot_without_avg["suppress_1f_num"] = np.where(
        pivot_without_avg["1F NUMERATOR"] < 5, 99999, pivot_without_avg["1F NUMERATOR"]
    )
    pivot_without_avg["suppress_1f_oc"] = np.where(
        pivot_without_avg["suppress_1f_num"] == 99999,
        99999,
        pivot_without_avg["1F SCORE"],
    )
    pivot_without_avg["suppress_denom"] = np.where(
        pivot_without_avg["DENOMINATOR"] < 5, 99999, pivot_without_avg["DENOMINATOR"]
    )
    old = ["1F NUMERATOR", "1F SCORE", "DENOMINATOR"]
    for o in old:
        del pivot_without_avg[o]
    pivot_without_avg.rename(
        columns={
            "suppress_1f_num": "1F NUMERATOR",
            "suppress_1f_oc": "1F SCORE",
            "suppress_denom": "DENOMINATOR",
        },
        inplace=True,
    )
    all_data = pd.concat([pivot_without_avg, pivot_with_avg])
    stacked = all_data.stack(level=0).reset_index()
    rename = stacked.rename(columns={0: "METRIC_VALUE"})
    return rename


def suppress_overall_avg(oc_avg, num_average):
    avg_all_data = pd.concat([oc_avg, num_average])
    avg_all_data_num = avg_all_data.loc[avg_all_data["Month"] == " Average"]
    pivot_with_avg_oc = avg_all_data.loc[avg_all_data["Month"] == "Num_Average"]
    all_avg = pd.concat([avg_all_data_num, pivot_with_avg_oc])
    pivot_with_avg = all_avg.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR Code", "CASSR", "ONS Code", "Gender"],
        columns=["METRIC", "Month"],
    )

    """0.416 and 99999 are suppresion and placeholder values that are transformed later in the pipeline"""

    pivot_with_avg["suppress_1f_num"] = np.where(
        pivot_with_avg["1F NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F NUMERATOR"]
    )
    pivot_with_avg["suppress_1f_oc"] = np.where(
        pivot_with_avg["1F NUMERATOR"] < 0.41666, 99999, pivot_with_avg["1F SCORE"]
    )
    old = ["1F NUMERATOR", "1F SCORE"]
    for o in old:
        del pivot_with_avg[o]
    pivot_with_avg.rename(
        columns={
            "suppress_1f_num": "1F NUMERATOR",
            "suppress_1f_oc": "1F SCORE",
        },
        inplace=True,
    )
    stacked = pivot_with_avg.stack(level=0).reset_index()
    rename = stacked.rename(columns={"": "METRIC_VALUE"})
    onef_score = rename.loc[rename["METRIC"] == "1F SCORE"]
    oneh_score = rename.loc[rename["METRIC"] == "1H SCORE"]
    outcome = pd.concat([onef_score, oneh_score])
    outcome["Month"] = " Average"
    return outcome


def rename_values(tidy_up_df):
    tidy_up_df["METRIC"] = tidy_up_df["METRIC"].replace(
        {"1F SCORE": "OUTCOME", "1F NUMERATOR": "NUMERATOR"}
    )
    return tidy_up_df


def pivotted_onef(super_clean):
    pivot = super_clean.pivot_table(
        values="METRIC_VALUE",
        index=["CASSR Code", "CASSR", "ONS Code"],
        columns=["Month", "Gender", "METRIC"],
    )
    return pivot


def onef_main():
    source_data = import_la_data()
    council_england = cleaning_months_together(months_together_input)
    cleaned_onef = clean_indicator_onef(council_england)
    merge = merge_council_type_region(cleaned_onef, source_data)
    filtered_num_denom = filter_num_denom(merge)
    filtered_score = filter_score(merge)
    calc_1 = calculate_MH_score_ASCOF_measures_by_regional_breakdown(filtered_score)
    calc_2 = calculate_MH_denom_num_measures_by_regional_breakdown(filtered_num_denom)
    type_01 = clean_type(calc_1, source_data)
    type_02 = clean_type(calc_2, source_data)
    region_01 = clean_region(calc_1, source_data)
    region_02 = clean_region(calc_2, source_data)
    council = clean_council(merge, source_data)
    england = clean_england(cleaned_onef)
    conc = concat(type_01, type_02, region_01, region_02, council, england)
    average_oc = average_outcome_indicator(conc)
    average_num = average_numerator(conc)
    suppress_num_denom = suppressed_num_denom_oc(conc)
    suppress_overall_oc = suppress_overall_avg(average_oc, average_num)
    merged_suppression = pd.concat([suppress_num_denom, suppress_overall_oc])
    rounded = applying_round(merged_suppression)
    tidy = rename_values(rounded)
    pivot = pivotted_onef(tidy)
    final = replace(pivot)
    return final
