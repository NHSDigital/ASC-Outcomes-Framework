

import pandas as pd
import numpy as np
from decimal import Decimal
from typing import NamedTuple
from ascof.HES import main as create_HES_data
from ascof.ONS import main as create_ONS_data
from ascof import params
from ascof.rounding import cleaning

HES_data = create_HES_data()
ONS_data = create_ONS_data()


"""This creates the combined ONS (office for National statistics), HES (Hospital Episode Statistics) and SALT(Short and Long term) data sources"""




def import_data():
    all_salt_data = params.all_salt_data
    return all_salt_data


def melt_uuid_info(all_salt_data):
    melted_salt_uuid_assets = all_salt_data.asset.melt(
        id_vars=["UUID"], var_name="Lacode", value_name="Value"
    )
    return melted_salt_uuid_assets


def merge_salt_reference(melted_salt_uuid_assets, all_salt_data):
    ascof_measures = pd.merge(
        melted_salt_uuid_assets, all_salt_data.ref, on="UUID", how="inner"
    )
    cleaned_ascof_measures = ascof_measures.loc[
        (ascof_measures["Value"] != "ItemValue")
    ]
    cleaned_ascof_measures["Value"] = cleaned_ascof_measures["Value"].astype("int64")
    cleaned_ascof_measures["Lacode"] = cleaned_ascof_measures["Lacode"].str.replace(
        r"\D", ""
    )
    return cleaned_ascof_measures


def pivot_ascof_values(cleaned_ascof_measures):
    pivoted_ascof_measures = cleaned_ascof_measures.pivot_table(
        values="Value",
        index=["Lacode", "Disaggregation", "Indicator"],
        columns="Type",
        aggfunc="sum",
    )
    pivoted_ascof_measures = pivoted_ascof_measures.reset_index()
    # calculate HES DATA
    HES_data["Lacode"] = HES_data["Lacode"].replace("U6Q5Z", "100")
    HES_data["Lacode"] = HES_data["Lacode"].replace("Z9D4Z", "101")

    pivoted_ascof_measures["Lacode"] = pivoted_ascof_measures["Lacode"].astype(str)
    HES_data["Lacode"] = HES_data["Lacode"].astype(str)
    pivoted_ascof_measures["Disaggregation"] = pivoted_ascof_measures[
        "Disaggregation"
    ].astype(str)
    HES_data["Disaggregation"] = HES_data["Disaggregation"].astype(str)

    HES = pivoted_ascof_measures[pivoted_ascof_measures["Indicator"] == "2B2"]
    del HES["Denominator"]
    del HES["Indicator"]
    merged = HES.merge(HES_data, how="inner", on=["Lacode", "Disaggregation"])

    # filter out 2A
    ONS = pivoted_ascof_measures[
        pivoted_ascof_measures["Indicator"].isin(["2A1", "2A2"])
    ]
    del ONS["Denominator"]
    merged_ONS = ONS.merge(ONS_data, how="inner", on=["Lacode", "Indicator"])
    # filter out 2b2
    Not_2b2 = pivoted_ascof_measures[
        ~pivoted_ascof_measures["Indicator"].isin(["2A1", "2A2", "2B2"])
    ]
    concat = pd.concat([Not_2b2, merged, merged_ONS])

    concat = concat.fillna(0.0)
    concat["Actual_Numerator"] = concat["Numerator"] - concat["Minus_from_Numerator"]
    del concat["Numerator"]
    del concat["Minus_from_Numerator"]
    concat.rename(columns={"Actual_Numerator":"Numerator"}, inplace = True)
    return concat


def merge_with_councils(pivoted_ascof_measures, all_salt_data):
    merged_ascof_measures = pd.merge(
        pivoted_ascof_measures,
        all_salt_data.council,
        left_on="Lacode",
        right_on="Council",
        how="inner",
    )
    return merged_ascof_measures


def format_disaggregation(cleaned_ascof_measures):
    cleaned_ascof_measures["Disaggregation"] = cleaned_ascof_measures[
        "Disaggregation"
    ].str.replace(r"-", "")
    return cleaned_ascof_measures


def calculate_ascof_measures_by_regional_breakdown(cleaned_salt_data):
    regional_breakdown_df_by_name = {}
    for regional_breakdown in ["Lacode", "Region", "Council Type"]:
        regional_breakdown_df_by_name[regional_breakdown] = cleaned_salt_data.groupby(
            [regional_breakdown, "Disaggregation", "Indicator"]
        )[["Denominator", "Numerator"]].sum()
    return regional_breakdown_df_by_name


def calculate_england(cleaned_ascof_measures):
    Indicators = cleaned_ascof_measures["Indicator"].unique()
    disaggregations = [
        "1864",
        "65OV",
        "TOTAL",
        "FEMALE",
        "MALE",
        "6584",
        "85OV",
        "6574",
        "7584",
    ]
    all_indicators = pd.DataFrame([])
    for i in Indicators:
        measure_cleaned_ascof_measures = cleaned_ascof_measures[
            cleaned_ascof_measures["Indicator"] == i
        ]
        for d in disaggregations:
            total_cleaned_ascof_measures = measure_cleaned_ascof_measures[
                measure_cleaned_ascof_measures["Disaggregation"] == d
            ]
            total_cleaned_ascof_measures[
                "Total Denominator"
            ] = total_cleaned_ascof_measures["Denominator"].sum()
            total_cleaned_ascof_measures[
                "Total Numerator"
            ] = total_cleaned_ascof_measures["Numerator"].sum()
            total_cleaned_ascof_measures["Outcome"] = (
                total_cleaned_ascof_measures["Total Numerator"]
                / total_cleaned_ascof_measures["Total Denominator"]
            ) * 100
            all_indicators = pd.concat([all_indicators, total_cleaned_ascof_measures])
            all_indicators = all_indicators[
                [
                    "Disaggregation",
                    "Indicator",
                    "Total Numerator",
                    "Total Denominator",
                    "Outcome",
                ]
            ]
    all_indicators = all_indicators.drop_duplicates()
    all_indicators = pd.melt(
        all_indicators,
        id_vars=["Disaggregation", "Indicator"],
        var_name="Measure Type",
        value_name="Measure value",
    )
    all_indicators = cleaning(all_indicators)
    all_indicators["Geographical Code"] = "England"
    all_indicators["Geographical Description"] = "England"
    all_indicators["Geographical Level"] = "England"
    all_indicators["ONS Code"] = "E92000001"
    all_indicators = all_indicators.rename(columns={"Indicator": "Measure Group"})
    return all_indicators


def clean_council(regional_breakdown_df_by_name, import_data):
    La = regional_breakdown_df_by_name["Lacode"].reset_index()
    La["Outcome"] = (La["Numerator"] / La["Denominator"]) * 100

    Hes = ["2B2"]
    SALT = ["1E","1G"]
    
    Hes_suppressed = La[La['Indicator'].isin(Hes)]

    

    Hes_suppressed['Outcome'] = np.where(Hes_suppressed['Denominator'] < 8, 999999995, Hes_suppressed['Outcome'])

    Hes_suppressed['Denominator'] = np.where(Hes_suppressed['Denominator'] < 8, 999999995, Hes_suppressed['Denominator'])
    
    SALT_suppressed = La[La['Indicator'].isin(SALT)]

    Gender = ['MALE','FEMALE']
    Total = ['TOTAL']

    SALT_Gender_suppressed = SALT_suppressed [SALT_suppressed ['Disaggregation'].isin(Gender)]

    measures = ['Outcome','Numerator','Denominator']
    for m in measures:
       SALT_Gender_suppressed[m] = np.where(SALT_Gender_suppressed['Denominator'] < 5, 999999995, SALT_Gender_suppressed[m])

    SALT_Total_suppressed = SALT_suppressed [SALT_suppressed ['Disaggregation'].isin(Total)]
   
    measures = ['Numerator','Denominator']
    for m in measures:
        SALT_Total_suppressed[m] = np.where(SALT_Total_suppressed['Denominator'] < 5, 9999999995, SALT_Total_suppressed[m])

    exempt_indicators = ["2B2","1E","1G"]
    x_suppressed = La[~La['Indicator'].isin(exempt_indicators)]

    x_suppressed['Outcome'] = np.where(x_suppressed['Denominator'] == 0, 888888885, x_suppressed['Outcome'])

    all_suppressed = pd.concat([Hes_suppressed,SALT_Total_suppressed,SALT_Gender_suppressed,x_suppressed])

    La = pd.melt(
        all_suppressed,
        id_vars=["Lacode", "Disaggregation", "Indicator"],
        var_name="Measure Type",
        value_name="Measure value",
    )
    all_indicators = La
    all_indicators = cleaning(all_indicators)
    all_indicators["Geographical Code"] = all_indicators["Lacode"]
    # all_indicators['Geographical Description'] = 'England'
    all_indicators["Geographical Level"] = "Council"
    # all_indicators['ONS Code'] = 'E92000001'
    merge = pd.merge(
        import_data.laref,
        all_indicators,
        left_on="Council",
        right_on="Lacode",
        how="left",
    )
    # merge = merge.drop(merge.columns[['Type','Region','RegionName','CouncilTypeCode']],axis=1)
    x = ["Type", "Region", "RegionName", "CouncilTypeCode", "Lacode", "Council"]
    for a in x:
        del merge[a]
    merge.rename(
        columns={"Name": "Geographical Description", "Indicator": "Measure Group"},
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
            "Measure value",
            "Measure Group",
        ]
    ]
    return  merge


def clean_region(regional_breakdown_df_by_name, import_data):
    region = regional_breakdown_df_by_name["Region"].reset_index()
    region["Outcome"] = (region["Numerator"] / region["Denominator"]) * 100
    region = pd.melt(
        region,
        id_vars=["Region", "Disaggregation", "Indicator"],
        var_name="Measure Type",
        value_name="Measure value",
    )
    all_indicators = region
    all_indicators = cleaning(all_indicators)
    all_indicators["Geographical Code"] = all_indicators["Region"]
    # all_indicators['Geographical Description'] = 'England'
    all_indicators["Geographical Level"] = "Region"
    # all_indicators['ONS Code'] = 'E92000001'
    ref = import_data.region_ref.drop_duplicates()
    merge = pd.merge(ref, all_indicators, on="Region", how="left")
    merge.rename(columns={"Indicator": "Measure Group"}, inplace=True)
    merge = merge[
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
    return merge


def clean_type(regional_breakdown_df_by_name, import_data):
    type = regional_breakdown_df_by_name["Council Type"].reset_index()
    type["Outcome"] = (type["Numerator"] / type["Denominator"]) * 100
    type = pd.melt(
        type,
        id_vars=["Council Type", "Disaggregation", "Indicator"],
        var_name="Measure Type",
        value_name="Measure value",
    )
    all_indicators = type
    all_indicators = cleaning(all_indicators)
    # all_indicators['Geographical Code'] = all_indicators['Council Type']
    ref = import_data.type_ref.drop_duplicates()
    merge = pd.merge(
        ref,
        all_indicators,
        left_on="Geographical Description",
        right_on="Council Type",
        how="left",
    )
    merge.rename(columns={"Indicator": "Measure Group"}, inplace=True)
    merge = merge[
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
    return merge


def concatenate(council, england, region, type):
    concat = pd.concat([council, england, region, type])
    #concat["Measure value"] = concat["Measure value"].round(1)
    #ONS = concat[concat["Measure Group"]== ("2A1","2A2") & concat["Measure Type"]== "Outcome"]
    #measures = []
    ONS = concat[concat["Measure Group"].isin(["2A1","2A2"]) & concat["Measure Type"].isin(["Outcome"])]
    ONS['Measure value'] = ONS['Measure value'].astype(float) * 1000

    ONS_NUM_DENOM = concat[concat["Measure Group"].isin(["2A1","2A2"]) & concat["Measure Type"].isin(["Denominator","Numerator"])]
    ONS = pd.concat([ONS,ONS_NUM_DENOM])

    other_measures = concat[~concat['Measure Group'].isin(["2A1","2A2"])]
    all_measures = pd.concat([ONS,other_measures])
    
    return all_measures

def rounded(all_measures):
    outcomes = all_measures[all_measures['Measure Type']== "Outcome"]
    outcomes['Measure value'] = outcomes['Measure value'].round(1)

    hes = all_measures[all_measures['Measure Group'].isin(["2B2"]) &  all_measures['Measure Type'].isin(["Denominator"])]

    hes['Measure value'] = (5 * round(hes['Measure value'].astype(int)/5)).astype(int)

    all_other_measures = all_measures[~all_measures["Measure Type"].isin(["Outcome"]) & ~(all_measures["Measure Group"].isin(["2B2"]) & all_measures["Measure Type"].isin(["Denominator"]))]

    all_other_measures["Measure value"] = all_other_measures["Measure value"].astype(int)

    final_measures = pd.concat([all_other_measures,hes,outcomes])

    #final_measures['Measure value'] = final_measures['Measure value'].astype(float)

    final_measures['Measure value'] = np.where(final_measures['Measure value'] == 999999995, "[c]", final_measures['Measure value'] )
    final_measures['Measure value'] = np.where(final_measures['Measure value'] == "888888885.0", "[x]", final_measures['Measure value'] )
    final_measures.to_csv(params.output_path + "salt_ascof(python).csv")
    return final_measures


def create_ascof_measures():
    sourcedata = import_data()
    melted = melt_uuid_info(sourcedata)
    merged_with_salt_reference = merge_salt_reference(melted, sourcedata)
    pivoted = pivot_ascof_values(merged_with_salt_reference)
    merged_with_councils = merge_with_councils(pivoted, sourcedata)
    format_the_disagregation = format_disaggregation(merged_with_councils)
    calc = calculate_ascof_measures_by_regional_breakdown(format_the_disagregation)
    england = calculate_england(format_the_disagregation)
    council = clean_council(calc, sourcedata)
    region = clean_region(calc, sourcedata)
    council_type = clean_type(calc, sourcedata)
    conc = concatenate(council, england, region, council_type)
    #conc['Measure value'] = conc['Measure value'].astype(int)
    rounding = rounded(conc)
    #rounding_to_5 = round_to_5(conc)
    return rounding



