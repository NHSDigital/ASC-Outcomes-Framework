import pandas as pd
from ascof import params
from ascof.rounding import my_round
import numpy as np

def import_data():
    
    ascs_ascof_data = params.ascs_ascof_data
    
    return ascs_ascof_data

    
def calc_council(ascs_ascof_data):
    council_data = ascs_ascof_data["council_data"]
    measures = ['numerator', 'denominator', 'outcome', 'margin_of_error','respondents','base']
    for m in measures:
        council_data.loc[council_data['suppress'] == "[c]", m] = "[c]"
    del council_data['suppress']
    del council_data['Unnamed: 0']
    del council_data['demographic']
    
    melt = pd.melt(ascs_ascof_data['council_data'], id_vars=['LaCode','demographic_value','measure'], var_name="Measure Type", value_name= "Measure value")
    return melt

def calc_council_total(ascs_ascof_data):
    council_total = ascs_ascof_data["council_data_total"]
    council_total['demographic_value'] = 'Total'
    measures = ['numerator', 'denominator', 'outcome', 'margin_of_error','respondents','base']
    for m in measures:
        council_total.loc[council_total['suppress'] == "[c]", m] = "[c]"
    del council_total['suppress']
    del council_total['Unnamed: 0']
    #del council_data['demographic']
    melt = pd.melt(council_total, id_vars=['LaCode','measure','demographic_value'], var_name="Measure Type", value_name= "Measure value")
    return melt

def concatenate_councils(council,council_total):
    concat = pd.concat([council,council_total])
    return concat

def calc_region(ascs_ascof_data):
    region = ascs_ascof_data["region_data"]
    
    
    measures = ['numerator', 'denominator', 'outcome', 'margin_of_error','respondents','base']
    for m in measures:
        region.loc[region['suppress'] == "[c]", m] = "[c]"
    del region['suppress']
    del region['Unnamed: 0']
    del region['demographic']
    
    melt = pd.melt(region, id_vars=['average_group','measure','demographic_value'], var_name="Measure Type", value_name= "Measure value")
    return melt


def calc_region_total(ascs_ascof_data):
    region_total = ascs_ascof_data["region_data_with_moe"]
    region_total['demographic_value'] = 'Total'
    measures = ['numerator', 'denominator', 'outcome', 'margin_of_error','respondents','base']
    for m in measures:
         region_total.loc[region_total['suppress'] == "[c]", m] = "[c]"
    del region_total['suppress']
    del region_total['Unnamed: 0']
    #del council_data['demographic']
    
    melt = pd.melt(region_total, id_vars=['average_group','demographic_value','measure'], var_name="Measure Type", value_name= "Measure value")
    return melt

def concatenate_regions(region, region_total, reference):
    concat = pd.concat([region, region_total])
    concat["average_group"] = concat["average_group"].replace(
        "Eastern", "East of England"
    )
    concat["average_group"] = concat["average_group"].replace(
        "Yorkshire And The Humber", "Yorkshire and The Humber"
    )
    merged = concat.merge(
        reference,
        left_on="average_group",
        right_on="Geographical Description",
        how="left",
    )
    del merged["average_group"]
    del merged["ONS Code"]
    del merged["Geographical Level"]
    return merged


def all_concat(region, council, reference):
    council.rename(columns={"LaCode": "Geographical Code"}, inplace=True)
    all_concatenated = pd.concat([region, council])
    merged = all_concatenated.merge(reference, on="Geographical Code", how="left")
    del merged["Geographical Description_x"]
    merged.rename(
        columns={"Geographical Description_y": "Geographical Description"},
        inplace=True,
    )
    return merged


def final_clean(rounded_df):
    rounded_df.rename(
        columns={
            "measure": "Measure Group",
            "demographic_value": "Disaggregation",
        },
        inplace=True,
    )
    rounded_df["Measure Group"] = rounded_df["Measure Group"].str[6:]
    rounded_df = rounded_df[
        [
            "Geographical Code",
            "Geographical Description",
            "Geographical Level",
            "ONS Code",
            "Disaggregation",
            "Measure Type",
            "Measure value",
            "Measure Group",
        ]
    ]
    rounded_df["Disaggregation"] = rounded_df["Disaggregation"].replace("True", "18-64")
    rounded_df["Disaggregation"] = rounded_df["Disaggregation"].replace(
        "False", "65 and over"
    )
    rounded_df["Measure Group"] = rounded_df["Measure Group"].replace("1L1", "1I1")
    return rounded_df


def ascof_measure_code(ascs_df):
    gender = ["Total", "Male", "Female"]
    age = ["65 and over", "18-64"]
    filtered_gender = ascs_df[ascs_df["Disaggregation"].isin(gender)]
    filtered_age = ascs_df[ascs_df["Disaggregation"].isin(age)]
    filtered_age["renamed_disag"] = filtered_age["Disaggregation"]
    filtered_age["renamed_disag"] = filtered_age["renamed_disag"].replace(
        {"65 and over": "65OV", "18-64": "1864"}
    )
    filtered_age["ASCOF Measure Code"] = (
        filtered_age["Measure Group"] + filtered_age["renamed_disag"]
    )
    filtered_age = filtered_age.drop(columns=["renamed_disag"])
    filtered_gender["ASCOF Measure Code"] = (
        filtered_gender["Measure Group"] + filtered_gender["Disaggregation"].str[0]
    )
    mask_1 = filtered_gender["ASCOF Measure Code"].str.contains("T")
    filtered_gender["ASCOF Measure Code"] = np.where(
        mask_1,
        filtered_gender["ASCOF Measure Code"].str.slice(0, 2),
        filtered_gender["ASCOF Measure Code"],
    )
    merge_gender_age = pd.concat([filtered_age, filtered_gender])
    return merge_gender_age


def rounding(merge_gender_age):
    five_rounded_measures = ["denominator","numerator","base"]
    one_rounded_measure = ["outcome","margin_of_error"]
    #suppress = "[c]"

    the_suppressed = merge_gender_age[merge_gender_age['Measure value']== "[c]"]
    merge_gender_age = merge_gender_age[merge_gender_age['Measure value']!= "[c]"]
    five_rounded = merge_gender_age[merge_gender_age["Measure Type"].isin(five_rounded_measures)]
    one_rounded = merge_gender_age[merge_gender_age["Measure Type"].isin(one_rounded_measure)]
    
    five_rounded['Measure value'] = (5 * round(five_rounded['Measure value'].astype(int)/5)).astype(int)
    one_rounded['Measure value'] = one_rounded['Measure value'].apply(my_round)

    five_rounded_and_one_rounded = pd.concat([five_rounded,one_rounded])
    suppressed_and_unsuppressed = pd.concat([the_suppressed,five_rounded_and_one_rounded])
    return suppressed_and_unsuppressed



def main():
    imports = import_data()
    council = calc_council(imports)
    council_total = calc_council_total(imports)
    concat_councils = concatenate_councils(council, council_total)
    region = calc_region(imports)
    region_total = calc_region_total(imports)
    concat_regions = concatenate_regions(
        region, region_total, imports["ascof_reference"]
    )
    concatenating = all_concat(
        concat_regions, concat_councils, imports["ascof_reference"]
    )
   
    final = final_clean(concatenating)
    measure = ascof_measure_code(final)
    round = rounding(measure)
    return round.fillna("[z]")

