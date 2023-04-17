import os
import glob
import pandas as pd
from ascof import params


"""This loops through all the mental health monthly data and appends them together to produce one full year of mental health data."""

def add_all_months():
    os.chdir(params.latest_months)
    #list all the files from the directory with .csv
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
    combined_csv.to_csv("final_months_together.csv", index=False)
    return combined_csv

add_all_months()