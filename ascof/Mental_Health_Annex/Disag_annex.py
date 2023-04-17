import pandas as pd
from ascof import params

CSV = params.CSV

"""This produces the disaggregated annex spreadsheet."""

def cleaned_csv(csv_df):
    indicator = ["1A", "1B", "1D", "1I1", "1I2", "3B", "3C", "3D1", "3D2", "4A", "4B"]
    removed_num_denom = csv_df[csv_df["Measure Group"].isin(indicator)]
    not_removed_num_denom = csv_df[~csv_df["Measure Group"].isin(indicator)]
    filtered_out_num_denom = removed_num_denom.loc[
        (removed_num_denom["Measure Type"] == "Outcome")
        | (removed_num_denom["Measure Type"] == "Base")
        | (removed_num_denom["Measure Type"] == "Margin Of Error")
    ]
    cleaned_csv = pd.concat([filtered_out_num_denom, not_removed_num_denom])
    super_clean_csv = cleaned_csv.sort_values(by="Measure Group", ascending=True)
    return super_clean_csv

def write(super_clean):


    writer = pd.ExcelWriter(params.output_path+"disag_annex.xlsx", engine="xlsxwriter")
    for indicator in super_clean["Measure Group"].unique():
        new_csv = super_clean[super_clean["Measure Group"] == indicator]
        pivot = pd.pivot_table(
            new_csv,
            values="Measure_Value",
            index=["Geographical Code", "Geographical Description", "ONS Code"],
            columns=["Disaggregation Level", "Measure Type"],
            aggfunc="first",
        )
        pivot.to_excel(writer, sheet_name=indicator)

    writer.close()

    return pivot

def main():
    clean = cleaned_csv(CSV)
    excel = write(clean)
    return excel



