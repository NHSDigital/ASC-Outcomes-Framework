import pandas as pd
from ascof.Mental_Health_Annex.ONEF import onef_main
from ascof.Mental_Health_Annex.ONEH import oneh_main
from ascof import params

"""This outputs the mental health annex spreadsheet, which contains the 1F sheet, 1H sheet, as well as the combined sheet with the gender breakdown"""


def mental_health_output():
  one_f = onef_main()
  one_h = oneh_main()
  onef_oneh_cassr = pd.read_csv(params.output_path+"MH_1F_1H_CASSR.csv")

  writer = pd.ExcelWriter(params.output_path+"MH_1F_1H.xlsx")
  one_f.to_excel(writer, sheet_name="1F")
  one_h.to_excel(writer, sheet_name="1H")
  onef_oneh_cassr.to_excel(writer, sheet_name="1F_1H CASSR", index=False)

  writer.close()
  return onef_oneh_cassr
