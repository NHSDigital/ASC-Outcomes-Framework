import pandas as pd
import numpy as np
import math
from decimal import Decimal, ROUND_HALF_UP

"""Contains all rounding and cleaning functions specifically created for social care analysis"""


def my_round5(n):

    """This rounds to the nearest 5"""

    lower = (n // 5) * 5
    upper = lower + 5

    if (n - lower) < (upper - n):
        return int(lower)
    return int(upper)


def my_round(n, ndigits=1):
    """This rounds to 1 decimal place"""

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


def my_round3(n, ndigits=3):


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

def cleaning(all_indicators):
        all_indicators["Disaggregation"] = np.where(
        all_indicators["Disaggregation"] == "FEMALE",
        "F",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        all_indicators["Disaggregation"] == "MALE",
        "M",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        all_indicators["Disaggregation"] == "TOTAL",
        "",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        all_indicators["Indicator"] == "2A1",
        "",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        all_indicators["Indicator"] == "2A2",
        "",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        (all_indicators["Indicator"] == "1C1B")
        & (all_indicators["Disaggregation"] == "1864"),
        "64U",
        all_indicators["Disaggregation"],
    )
        all_indicators["Disaggregation"] = np.where(
        (all_indicators["Indicator"] == "1C2B")
        & (all_indicators["Disaggregation"] == "1864"),
        "64U",
        all_indicators["Disaggregation"],
    )
        all_indicators["ASCOF Measure Code"] = (
        all_indicators["Indicator"] + all_indicators["Disaggregation"]
    )
        return all_indicators