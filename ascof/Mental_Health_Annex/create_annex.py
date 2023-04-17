import pandas as pd


"""This creates all the Mental Health and ASCOF annex tables which are sepearte from the main csv."""

from ascof.Mental_Health_Annex.Disag_annex import main as disag
#from ascof.Mental_Health_Annex.MH_input import add_all_months as all_months
from ascof.Mental_Health_Annex.ONEF_and_ONEH import onef_oneh_cassr_main
from ascof.Mental_Health_Annex.Output import mental_health_output as mh_out


def annex():

  disag()
  #all_months()
  onef_oneh_cassr_main()
  mh_out()
  return

annex()