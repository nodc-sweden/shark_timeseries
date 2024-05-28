# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 15:36:41 2017

@author: a002087

Methods to plot profile data

"""
import numpy as np
import math
import gsw
import seawater


def get_prio_par_oxy(BTL, CTD, q_prio1_par='', q_prio2_par=''):

    if not np.isnan(BTL) and q_prio1_par not in ['B']:
        return BTL
    elif not np.isnan(CTD) and q_prio2_par not in ['B']:
        if CTD < 0.2:
            return np.nan
        else:
            return CTD
    else:
        return np.nan


def get_prio_par(prio1_par, prio2_par, q_prio1_par='', q_prio2_par=''):

    if not np.isnan(prio1_par) and q_prio1_par not in ['B']:
        return prio1_par
    elif  q_prio2_par not in ['B']:
        return prio2_par
    else:
        return np.nan


def get_sumNOx(nox, no3, no2, qnox, qno3, qno2):

    if not np.isnan(nox):  # and (qnox not in [u'?',u'S',u'B']):
        return nox
    elif not np.isnan(no3):  # and (qno3 not in [u'?',u'S',u'B']):
        if not np.isnan(no2) and (qno2 not in ["?", "S", "B", "<"]):
            return no3 + no2
        else:
            return no3
    else:
        return np.nan


def get_org(tot, inorg, qinorg, qtot):

    if (
        (not np.isnan(tot))
        and (not np.isnan(inorg))
        and (qinorg not in ["?", "S", "B"])
        and (qtot not in ["?", "S", "B"])
    ):
        return tot - inorg
    else:
        return np.nan


def get_din_simple(sumNOx, nh4):

    if any((np.isnan(nh4), np.isnan(sumNOx))):
        return np.nan
    else:
        return nh4 + sumNOx


def get_din_complex(
    no2, no3, nox, nh4, h2s, qh2s, qnh4, qnox, qno3, qno2, ignore_qf=["?", "B", "S"]
):
    """
    Returns a vector calculated DIN.
    If H2S is present NH4 is returned
    If NO3 is not present value is np.nan
    If no H2S and NH4 qflag is < nox is returned
    """

    if (
        not np.isnan(h2s)
        and h2s >= 4
        and any([qh2s not in ["<", "B"], (qh2s == "<" and qno3 in ["B", "<"])])
    ):

        if any([np.isnan(nh4), qnh4 in ["B"]]):
            din = np.nan
        else:
            din = nh4

        if not np.isnan(nox) and qnox not in ["B", "<"]:
            din += nox
        else:
            if not np.isnan(no3) and qno3 not in ["B", "<"]:
                din += no3
            if not np.isnan(no2) and qno2 not in ["B", "<"]:
                din += no2

    else:
        if np.isnan(nox):
            din = np.nan

            if not np.isnan(no3):
                if qno3 in ["B"]:
                    din = np.nan
                else:
                    din = no3

                if not np.isnan(no2) and not np.isnan(din):
                    din += no2
        else:
            din = nox

        if not np.isnan(nh4) and qnh4 not in ["<", "B"] and not np.isnan(din):
            din += nh4

    return din


def get_zero_H2S(o2, h2s, qo2=None, qh2s=None):
    """
    returns H2S profile padded with zeros where O2 and qflags indicates that there is no H2S
    """
    # no O2 or H2S data
    if np.isnan(o2) and np.isnan(h2s):
        h2s_zero = np.nan
        h2s_zero_qf = ""
    # O2 measured but no H2S data
    elif not np.isnan(o2) and np.isnan(h2s):
        h2s_zero_qf = ""
        # O2 data not good
        if qo2 in ["B", "?", "Z"]:
            h2s_zero = np.nan
        # O2 data above 0, accepting < qflags
        elif o2 > 0:
            h2s_zero = 0
            h2s_zero_qf = qo2
        else:
            h2s_zero = np.nan
            h2s_zero_qf = qo2
    # O2 and H2S measured
    elif not np.isnan(o2) and not np.isnan(h2s):
        # O2 data set to zero
        if qo2 == "Z" or o2 == 0:
            h2s_zero_qf = qh2s
            # H2S data of poor quality
            if qh2s in ["?", "S", "B"]:
                h2s_zero = np.nan
            else:
                h2s_zero = h2s
        # both below det lim set H2S to half of reported value
        elif qo2 == "<" and qh2s == "<":
            h2s_zero = h2s / 2
            h2s_zero_qf = qh2s
        # good O2 data and H2S below det lim
        elif qo2 == "" and qh2s == "<":
            h2s_zero = 0
            h2s_zero_qf = qo2
        # bad H2S data and ok O2 data
        elif (qh2s in ["?", "S", "B"]) and (
            qo2 not in ["?", "S", "B", "Z"] and o2 != 0
        ):
            h2s_zero = 0
            h2s_zero_qf = qo2
        # good O2 data and H2S at or below detectionlimit
        elif (qh2s == "<" and h2s == 4) and (
            qo2 not in ["?", "S", "B", "<"] and o2 != 0
        ):
            h2s_zero = 0  # Added by Johannes 10/8 due to H2S values of <4.. # men här kan det inte sättas till o2 utan ska vara 0
            h2s_zero_qf = qo2
        # H2S set to zero in old-time analysis
        elif h2s == 0 or qh2s == "Z":
            h2s_zero = 0
            h2s_zero_qf = ""
        else:
            h2s_zero = h2s
            h2s_zero_qf = qh2s
    # H2S measured but no O2 data
    elif np.isnan(o2) and not np.isnan(h2s):
        h2s_zero_qf = qh2s
        # H2S data is bad
        if qh2s == "<":
            h2s_zero = h2s / 2
        elif qh2s in ["?", "S", "B"]:
            h2s_zero = np.nan
        else:
            h2s_zero = h2s
    # H2S measured, this statement should never be reached....
    elif not np.isnan(h2s):
        if qh2s in ["?", "S", "B"]:
            h2s_zero = np.nan  # or (qo2 in ['?','S','B']):  Added by Johannes 10/8
            h2s_zero_qf = qh2s
        else:
            h2s_zero = h2s
            h2s_zero_qf = qh2s
    # If none of the above statements are true
    else:
        h2s_zero = np.nan
        h2s_zero_qf = qh2s

    return h2s_zero

def get_string_qf_from_float_qf(qf):

    if np.isnan(qf):
        return ''
    return str(qf)

def get_O2_H2S(o2, h2s, qo2='', qh2s=''):
    """
    returns the oxygen or negative oxygen (H2S*-0.04488) where appropriate according to data and qflags
    """

    # first some data quality checks

    # this exists in data from the 1980's
    if qh2s == '>' and h2s == 0 and o2 == 0:
        return np.nan
    
    if qh2s == '<' and h2s == 79:
        return h2s * (-0.04488)

    # if quality flag is float and the float is nan set to string
    if isinstance(qh2s, float):
        qh2s = get_string_qf_from_float_qf(qh2s)
    if isinstance(qo2, float):
        qo2 = get_string_qf_from_float_qf(qo2)

    neg_o2 = h2s * (-0.04488)
    # no O2 or H2S data
    if np.isnan(o2) and np.isnan(h2s):
        o2_h2s = np.nan
        o2_h2s_qf = qo2
        return o2_h2s

    # O2 measured but no H2S data
    if not np.isnan(o2) and np.isnan(h2s):
        o2_h2s_qf = ""
        # O2 data not good
        if qo2 in ["B", "?"]:
            return np.nan
        else:
            return o2
    # O2 and H2S measured
    if not np.isnan(o2) and not np.isnan(h2s):
        # both without bad flags
        if qo2 in ['', 'E', 'R', 'S'] and qh2s in ['', 'E', 'R', 'S']:
            if h2s >= 4:
                return neg_o2
            return o2
        # both below det lim
        if qo2 == "<" and qh2s == "<":
            return neg_o2
            o2_h2s_qf = qh2s
        # good O2 data that is not set to zero and H2S below detection limit
        if qo2 in ["", np.nan] and qh2s in ["<", "Z"] and o2 != 0:
            return o2
            o2_h2s_qf = qo2
        # good H2S data that is not set to zero and H2S below detection limit
        if qh2s in ["", np.nan, "E"] and qo2 in ["<", "Z"] and h2s != 0:
            return neg_o2
        # bad H2S data and zero O2 data. Not possible to decide if O2 and H2S is zero or O2=0 and H2S>0
        if (qh2s in ["?", "S", "B", ">"]) and o2 == 0:
            return np.nan  # h2s*(-0.04488)
            o2_h2s_qf = qh2s
        # bad H2S data and good O2 data that is not set to zero
        if (qh2s in ["?", "S", "B", ">"]) and (
            qo2 not in ["?", "S", "B", "Z"] and o2 != 0
        ):
            return o2
            o2_h2s_qf = qo2
        # good O2 data and H2S at or below detection limit
        if (qh2s == "<" and h2s <= 4) and (
            qo2 not in ["?", "S", "B", "<"] and o2 != 0
        ):
            return o2  # Added by Johannes 10/8 due to H2S values of <4..
            o2_h2s_qf = qo2
        # good O2 and H2S data, H2S not zero
        if (qh2s not in ["?", "S", "B", "<", "E", ">", "Z"] and h2s != 0) and (
            qo2 not in ["?", "S", "B", "<", "E", ">", "Z"]
        ):
            return neg_o2
            o2_h2s_qf = qh2s
        # O2 set to zero in old-time analysis and H2S < flagged
        if o2 == 0 and (qh2s in ["<", ""] or isinstance(qh2s, float)):
            return neg_o2
            o2_h2s_qf = ""
        # H2S set to zero in old-time analysis
        if h2s == 0 or qh2s == "Z":
            return o2
            o2_h2s_qf = ""
        # O2 data suspect (S) and H2S data "<":
        if qo2 in ["S"] and qh2s in ["<"]:
            return np.nan
    # H2S measured but no O2 data
    if np.isnan(o2) and not np.isnan(h2s):
        o2_h2s_qf = qh2s
        # H2S data is bad
        if qh2s not in ["", "<"] or isinstance(qh2s, float):
            return np.nan
        else:
            return neg_o2

    raise ValueError(f"Did not find a solution for this combination, o2: {o2}, {qo2}. h2s: {h2s}, {qh2s}, {type(qh2s)}, {(qh2s in ['<', ''] or isinstance(qh2s, float))}")

def get_o2sat(o2, temperature, salinity, pressure, lat, lon):

    """
    Calculates the oxygen concentration expected at equilibrium with air at an Absolute Pressure of 101325 Pa 
    (sea pressure of 0 dbar) including saturated water vapor. 
    This function uses the solubility coefficients derived from the data of Benson and Krause (1984), 
    as fitted by Garcia and Gordon (1992, 1993).
    """
    o2sol =  gsw.O2sol(SA = salinity, CT = temperature, p = pressure, lat=lat, lon=lon)
    """
    unit conversion from https://www.nodc.noaa.gov/OC5/WOD/wod18-notes.html 
    1 ml/l of O2 is approximately 43.570 µmol/kg (assumes a molar volume of O2 of 22.392 l/mole and a constant seawater potential density of 1025 kg/m3).
    The conversion of units on a per-volume (e.g., per liter) to a per-mass (e.g., per kilogram) basis assumes a constant seawater potential density of 1025 kg/m3.
    """
    conversion_factor = 43.570
    o2sat_perc = (o2/(o2sol/conversion_factor))*100

    return o2sat_perc

def O2ctoO2s(O2conc, T, S, P = 0, p_atm = 1013.25):
    """    
    convert molar oxygen concentration to oxygen saturation
    
    inputs:
    O2conc - oxygen concentration in umol L-1
    T      - temperature in °C
    S      - salinity (PSS-78)
    P      - hydrostatic pressure in dbar (default: 0 dbar)
    p_atm  - atmospheric (air) pressure in mbar (default: 1013.25 mbar)
    
    output:
    O2sat  - oxygen saturation in #
    
    according to recommendations by SCOR WG 142 "Quality Control Procedures
    for Oxygen and Other Biogeochemical Sensors on Floats and Gliders"
    
    Henry Bittig
    Laboratoire d'Océanographie de Villefranche-sur-Mer, France
    bittig@obs-vlfr.fr
    28.10.2015
    19.04.2018, v1.1, fixed typo in B2 exponent
    """

    pH2Osat = 1013.25*(math.exp(24.4543-(67.4509*(100./(T+273.15)))-(4.8489*math.log(((273.15+T)/100)))-0.000544*S)) # saturated water vapor in mbar
    sca_T   = math.log((298.15-T)/(273.15+T)); # scaled temperature for use in TCorr and SCorr
    TCorr   = 44.6596*math.exp(2.00907+3.22014*sca_T+4.05010*sca_T**2+4.94457*sca_T**3-2.56847e-1*sca_T**4+3.88767*sca_T**5) # temperature correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit mL(STP) L-1; and conversion from mL(STP) L-1 to umol L-1
    Scorr   = math.exp(S*(-6.24523e-3-7.37614e-3*sca_T-1.03410e-2*sca_T**2-8.17083e-3*sca_T**3)-4.88682e-7*S**2) # salinity correction part from Garcia and Gordon (1992), Benson and Krause (1984) refit ml(STP) L-1
    Vm      = 0.317; # molar volume of O2 in m3 mol-1 Pa dbar-1 (Enns et al. 1965)
    R       = 8.314; # universal gas constant in J mol-1 K-1

    O2conc_umol = O2conc/44.661

    O2sat=O2conc_umol*100/(TCorr*Scorr)/(p_atm-pH2Osat)*(1013.25-pH2Osat)*math.exp(Vm*P/(R*(T+273.15)))

    return O2sat

def calculate_o2sat(o2, s, t):

    o2sat = o2 / seawater.satO2(s, t) * 100

    return o2sat
