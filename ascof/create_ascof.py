import pandas as pd
from ascof.ons_hes_salt import create_ascof_measures as salt
from ascof.ascs import main as ascs
from ascof.mental_health import MH_CSV_data_main as mh

"""THIS ONLY NEEDS TO BE RUN ONCE PER YEARS AS IT PRODUCES ALL THE MH FILES FOR THE YEAR. RUNNING THIS EVERYTIME WILL GREATELY INCREASE THE RUN TIME OF THE CODE"""
#from ascof.mental_health_annex import onef_oneh_cassr_main as mh_annex
from ascof import params
from ascof.Mental_Health_Annex.create_annex import annex


salt_data = salt()
ascs_data = ascs()
mh_data = mh()

annex()

all_ascof_ouputs = pd.concat([salt_data,ascs_data,mh_data])


all_ascof_ouputs.to_csv(params.output_path+"ascof_outputs.csv")
print(all_ascof_ouputs)