# A script for ABS data extraction
# Author: Brad Hershowitz
# Date: 16/03/2020

import os
import pandas as pd
import numpy as np
import time
import pickle
import sys
import warnings
import math
from itertools import repeat
import configparser

warnings.filterwarnings("ignore")

"""ABS Geographic Data Preparation

This script reads in a DataPack (which is publically available
and free at https://www.abs.gov.au/websitedbs/D3310114.nsf/Home/2016%20DataPacks) and 
alligns a range of census features into one usable dataframe.

Intructions to Download:

    The following dropdown options should be selected before downloading the DataPack.

        Census Year: 2016
        DataPacks type: General Community Profile
        Geography: All geographies

    The following zip file should then be dowloaded to your device: '2016_GCP_ALL_for_AUS_short-header.zip'




Args:
    granularity: This levels indicates how specific you want your data to be.
        Choose from:
            POA (PostCode), SA1, SA2, SA3, SA4, SSC (State Suburb Code)

    index_code: This is the code specific to each geographic level. This will be the name of the first column in any of the files.
        Examples:
            POA has index_code: POA_CODE_2016
            SSC has index_code: SSC_CODE_2016
            SA1 had index_code: SA1_7DIGITCODE_2016

    dir: Where the DataPack is stored on your device
        Needs to be formatted as '~/2016 Census GCP All Geographies for AUST/'+granularity+'/AUST/'
        where ~ is your working directory.
"""

#=======================================================================
#PARAMATERS from config file
#=======================================================================
config = configparser.ConfigParser()
config.read('config.ini')

granularity = config.get('parameters','granularity')
index_code = config['parameters']['index_code']
directory = config['parameters']['dir']
dir = directory + '2016 Census GCP All Geographies for AUST/'+granularity+'/AUST/'


#=======================================================================
#FUNCTIONS
#=======================================================================

def snip(name):
    new = name[2:-4]
    return (new)

def median_range(val):
    if val == 0:
        return ('Neg_Nil_income')
    elif val >= 1 and val < 150:
        return ('1_149')
    elif val >= 150 and val < 300:
        return ('150_299')
    elif val >= 300 and val < 400:
        return ('300_399')
    elif val >= 400 and val < 500:
        return ('400_499')
    elif val >= 500 and val < 650:
        return ('500_649')
    elif val >= 650 and val < 800:
        return ('650_799')
    elif val >= 800 and val < 1000:
        return ('800_999')
    elif val >= 1000 and val < 1250:
        return ('1000_1249')
    elif val >= 1250 and val < 1500:
        return ('1250_1499')
    elif val >= 1500 and val < 1750:
        return ('1500_1749')
    elif val >= 1750 and val < 2000:
        return ('1750_1999')
    elif val >= 2000 and val < 3000:
        return ('2000_2999')
    else:
        return ('3000_more')


def get_int_vals(df,geolevel):
    lst = []
    for i in df.index.tolist()[1:]:
        upper_val = toint(i)
        pos = df.index.tolist()[1:].index(i)
        freq = df[geolevel].tolist()[1:][pos]
        lst.extend(repeat(upper_val, freq))
    return (lst)


def toint(str):
    if str == 'Neg_Nil_income':
        return (0)
    elif str == '1_149':
        return (75)
    elif str == '150_299':
        return (225)
    elif str == '300_399':
        return (350)
    elif str == '400_499':
        return (450)
    elif str == '500_649':
        return (575)
    elif str == '650_799':
        return (625)
    elif str == '800_999':
        return (850)
    elif str == '1000_1249':
        return (1125)
    elif str == '1250_1499':
        return (1375)
    elif str == '1500_1749':
        return (1625)
    elif str == '1750_1999':
        return (1875)
    elif str == '2000_2999':
        return (2500)
    else:
        return (3000)


def summary(df,geolevel):
    int_lst = get_int_vals(df,geolevel)
    info = pd.Series(int_lst).describe()
    se = pd.Series(get_int_vals(df,geolevel))
    se = (se - se.min()) / ((se.max() - se.min()))
    info_norm = se.describe()
    return (info, info_norm)


def get_str_vals_HI(df,geolevel):
    lst = []
    for i in df.index.tolist()[1:]:
        pos = df.index.tolist()[1:].index(i)
        freq = df[geolevel].tolist()[1:][pos]
        lst.extend(repeat(i, freq))
    return (lst)


def get_int_vals_HI(df,geolevel):
    lst = []
    for i in df.index.tolist()[1:]:
        upper_val = toint_HI(i)
        pos = df.index.tolist()[1:].index(i)
        freq = df[geolevel].tolist()[1:][pos]
        lst.extend(repeat(upper_val, freq))
    return (lst)


def toint_HI(str):
    if str == 'Negative_Nil_income_Tot':
        return (0)
    elif str == 'HI_1_149_Tot':
        return (75)
    elif str == 'HI_150_299_Tot':
        return (225)
    elif str == 'HI_300_399_Tot':
        return (350)
    elif str == 'HI_400_499_Tot':
        return (450)
    elif str == 'HI_500_649_Tot':
        return (575)
    elif str == 'HI_650_799_Tot':
        return (625)
    elif str == 'HI_800_999_Tot':
        return (850)
    elif str == 'HI_1000_1249_Tot':
        return (1125)
    elif str == 'HI_1250_1499_Tot':
        return (1375)
    elif str == 'HI_1500_1749_Tot':
        return (1625)
    elif str == 'HI_1750_1999_Tot':
        return (1875)
    elif str == 'HI_2000_2499_Tot':
        return (2250)
    elif str == 'HI_2500_2999_Tot':
        return (2750)
    elif str == 'HI_3000_3499_Tot':
        return (3250)
    elif str == 'HI_3500_3999_Tot':
        return (3750)
    else:
        return (4000)


def median_range_HI(val):
    if val == 0:
        return ('Negative_Nil_income_Tot')
    elif val >= 1 and val < 150:
        return ('HI_1_149_Tot')
    elif val >= 150 and val < 300:
        return ('HI_150_299_Tot')
    elif val >= 300 and val < 400:
        return ('HI_300_399_Tot')
    elif val >= 400 and val < 500:
        return ('HI_400_499_Tot')
    elif val >= 500 and val < 650:
        return ('HI_500_649_Tot')
    elif val >= 650 and val < 800:
        return ('HI_650_799_Tot')
    elif val >= 800 and val < 1000:
        return ('HI_800_999_Tot')
    elif val >= 1000 and val < 1250:
        return ('HI_1000_1249_Tot')
    elif val >= 1250 and val < 1500:
        return ('HI_1250_1499_Tot')
    elif val >= 1500 and val < 1750:
        return ('HI_1500_1749_Tot')
    elif val >= 1750 and val < 2000:
        return ('HI_1750_1999_Tot')
    elif val >= 2000 and val < 2500:
        return ('HI_2000_2499_Tot')
    elif val >= 2500 and val < 3000:
        return ('HI_2500_2999_Tot')
    elif val >= 3000 and val < 3500:
        return ('HI_3000_3499_Tot')
    elif val >= 3500 and val < 4000:
        return ('HI_3500_3999_Tot')
    else:
        return ('HI_4000_more_Tot')


def summary_HI(df,geolevel):
    int_lst = get_int_vals_HI(df,geolevel)
    info = pd.Series(int_lst).describe()
    se = pd.Series(get_int_vals_HI(df,geolevel))
    se = (se - se.min()) / ((se.max() - se.min()))
    info_norm = se.describe()
    return (info, info_norm)


# Weekly Income Personal
def Load_wkly_prsnl_inc():
    start_time = time.time()
    file1 = dir + '2016Census_G17A_AUS_' + granularity + '.csv'
    file2 = dir + '2016Census_G17B_AUS_' + granularity + '.csv'
    file3 = dir + '2016Census_G17C_AUS_' + granularity + '.csv'

    df_WeeklyIncomeA = pd.read_csv(file1)
    df_WeeklyIncomeB = pd.read_csv(file2)
    df_WeeklyIncomeC = pd.read_csv(file3)

    df_WeeklyIncome = pd.concat([df_WeeklyIncomeA, df_WeeklyIncomeB, df_WeeklyIncomeC], axis=1)
    df_WeeklyIncome = df_WeeklyIncome.loc[:, ~df_WeeklyIncome.columns.duplicated()]

    df_WeeklyIncome['GeoLevel'] = df_WeeklyIncome[index_code]
    df_WeeklyIncome = df_WeeklyIncome.set_index('GeoLevel')
    df_WeeklyIncome['GeoLevel'] = df_WeeklyIncome[index_code]

    Persons_Total_Cols = ['GeoLevel', 'P_Neg_Nil_income_Tot', 'P_1_149_Tot', 'P_150_299_Tot', 'P_300_399_Tot',
                          'P_400_499_Tot', 'P_500_649_Tot', 'P_650_799_Tot', 'P_800_999_Tot',
                          'P_1000_1249_Tot', 'P_1250_1499_Tot', 'P_1500_1749_Tot', 'P_1750_1999_Tot',
                          'P_2000_2999_Tot', 'P_3000_more_Tot']  # 'P_PI_NS_ns_Tot' - Personal Income not stated col
    Persons_Total_Cols_New = ['GeoLevel', 'Neg_Nil_income', '1_149', '150_299', '300_399', '400_499', '500_649', '650_799',
                              '800_999', '1000_1249', '1250_1499', '1500_1749', '1750_1999', '2000_2999', '3000_more']

    categories = {}
    for i in Persons_Total_Cols[1:]:
        categories.update({i: snip(i)})
    df_WeeklyIncome = df_WeeklyIncome.rename(columns=categories, inplace=False)

    df_WeeklyIncome = df_WeeklyIncome[Persons_Total_Cols_New]

    df_WeeklyIncome_Transpose = df_WeeklyIncome[Persons_Total_Cols_New].transpose()

    data = {}
    geolevels_lst = df_WeeklyIncome_Transpose.columns.tolist()
    for gl in geolevels_lst:
        info_list = []
        if sum(df_WeeklyIncome_Transpose[gl].tolist()[1:]) == 0:
            data.update({gl: [gl, 0, 0, 0, 0]})
        else:
            info_list.append(gl)
            info_list.append(summary(df_WeeklyIncome_Transpose,gl)[0].loc['std'])
            info_list.append(summary(df_WeeklyIncome_Transpose,gl)[1].loc['std'])
            info_list.append(summary(df_WeeklyIncome_Transpose,gl)[0].loc['mean'])
            info_list.append(summary(df_WeeklyIncome_Transpose,gl)[1].loc['mean'])
            data.update({gl: info_list})

    df_overview = pd.DataFrame(data, columns=geolevels_lst, index=['GeoLevel', 'std', 'std_norm', 'mean', 'mean_norm'])

    df_overview = df_overview.transpose()

    # For null cells (caused by data with income of 0), normalised values need to be imputed as there was divide by zero error
    na_lst = df_overview[df_overview["std_norm"].isnull()]['GeoLevel'].tolist()
    df_overview = df_overview.transpose()
    for gl in na_lst:
        df_overview[gl].loc['std_norm'] = 0
        df_overview[gl].loc['mean_norm'] = df_overview[gl].loc['mean']

    print("Weekly Personal Income data processed in", (time.time() - start_time), "s\n")
    return(df_overview)

def Load_median_data(df_overview):
    start_time=time.time()
    # Process EXACT MEDIANS values

    file4 = dir + '2016Census_G02_AUS_' + granularity + '.csv'
    df_medians = pd.read_csv(file4)
    df_medians['GeoLevel'] = df_medians[index_code]
    df_medians = df_medians.set_index('GeoLevel')

    df_overview = df_overview.transpose()
    df_overview['Exact Median prsnl inc weekly'] = df_medians['Median_tot_prsnl_inc_weekly']
    df_overview['Median Total Personal Income (Weekly)'] = df_overview['Exact Median prsnl inc weekly'].apply(
        lambda x: median_range(x))
    df_overview = df_overview[
        ['GeoLevel', 'Exact Median prsnl inc weekly', 'Median Total Personal Income (Weekly)', 'std', 'std_norm',
         'mean',
         'mean_norm']]

    df_overview = df_overview.rename(columns={'Exact Median prsnl inc weekly': 'median_personal_weekly_income',
                                              'Median Total Personal Income (Weekly)': 'median_personal_weekly_income_interval',
                                              'std': 'std_personal_weekly_income',
                                              'std_norm': 'std_norm_personal_weekly_income',
                                              'mean': 'mean_personal_weekly_income',
                                              'mean_norm': 'mean_norm_personal_weekly_income'})

    df_overview['Median_mortgage_repay_monthly'] = df_medians['Median_mortgage_repay_monthly']
    df_overview['Median_rent_weekly'] = df_medians['Median_rent_weekly']
    df_overview['Median_tot_fam_inc_weekly'] = df_medians['Median_tot_fam_inc_weekly']
    df_overview['Average_num_psns_per_bedroom'] = df_medians['Average_num_psns_per_bedroom']
    df_overview['Average_household_size'] = df_medians['Average_household_size']
    df_overview['median_age_persons'] = df_medians['Median_age_persons']
    print("Median data processed in", (time.time() - start_time), "s\n")
    return(df_overview)
# Household Income
def Load_hhld_wkly_inc(df_overview):
    start_time=time.time()
    file5 = dir + '2016Census_G29_AUS_' + granularity + '.csv'
    df_hhld_income = pd.read_csv(file5)

    df_hhld_income['GeoLevel'] = df_hhld_income[index_code]

    df_hhld_income = df_hhld_income.set_index('GeoLevel')

    df_hhld_income['GeoLevel'] = df_hhld_income[index_code]

    hhld_Total_Cols = ['GeoLevel', 'Negative_Nil_income_Tot', 'HI_1_149_Tot', 'HI_150_299_Tot', 'HI_300_399_Tot',
                       'HI_400_499_Tot', 'HI_500_649_Tot', 'HI_650_799_Tot', 'HI_800_999_Tot',
                       'HI_1000_1249_Tot', 'HI_1250_1499_Tot', 'HI_1500_1749_Tot', 'HI_1750_1999_Tot',
                       'HI_2000_2499_Tot', 'HI_2500_2999_Tot', 'HI_3000_3499_Tot', 'HI_3500_3999_Tot',
                       'HI_4000_more_Tot']  # 'P_PI_NS_ns_Tot' - Personal Income not stated col

    df_hhld_income = df_hhld_income[hhld_Total_Cols]

    df_hhld_income_Transpose = df_hhld_income.transpose()

    data = {}
    geolevels_lst = df_hhld_income_Transpose.columns.tolist()
    for gl in geolevels_lst:
        info_list = []
        if sum(df_hhld_income_Transpose[gl].tolist()[1:]) == 0:
            data.update({gl: [gl, 0, 0, 0, 0]})
        else:
            info_list.append(gl)
            info_list.append(summary_HI(df_hhld_income_Transpose,gl)[0].loc['std'])
            info_list.append(summary_HI(df_hhld_income_Transpose,gl)[1].loc['std'])
            info_list.append(summary_HI(df_hhld_income_Transpose,gl)[0].loc['mean'])
            info_list.append(summary_HI(df_hhld_income_Transpose,gl)[1].loc['mean'])
            data.update({gl: info_list})
    df_overview2 = pd.DataFrame(data, columns=geolevels_lst,
                                index=['GeoLevel', 'hhld_weekly_income_std', 'hhld_weekly_income_std_norm',
                                       'hhld_weekly_income_mean', 'hhld_weekly_income_mean_norm'])

    df_hhld_inc_weekly = df_overview2.transpose()

    file4 = dir + '2016Census_G02_AUS_' + granularity + '.csv'
    df_medians = pd.read_csv(file4)
    df_medians['GeoLevel'] = df_medians[index_code]
    df_medians = df_medians.set_index('GeoLevel')
    df_hhld_inc_weekly['Median_tot_hhd_inc_weekly'] = df_medians['Median_tot_hhd_inc_weekly']

    df_hhld_inc_weekly = df_hhld_inc_weekly[
        ['GeoLevel', 'Median_tot_hhd_inc_weekly', 'hhld_weekly_income_std', 'hhld_weekly_income_std_norm',
         'hhld_weekly_income_mean', 'hhld_weekly_income_mean_norm']]

    na_lst = df_hhld_inc_weekly[df_hhld_inc_weekly["hhld_weekly_income_std_norm"].isnull()]['GeoLevel'].tolist()
    df_hhld_inc_weekly = df_hhld_inc_weekly.transpose()
    for gl in na_lst:
        df_hhld_inc_weekly[gl].loc['hhld_weekly_income_std_norm'] = 0
        df_hhld_inc_weekly[gl].loc['hhld_weekly_income_mean_norm'] = df_hhld_inc_weekly[gl].loc['hhld_weekly_income_mean']
    df_hhld_inc_weekly = df_hhld_inc_weekly.transpose()

    df_hhld_inc_weekly['hhld_weekly_income_median_interval'] = df_hhld_inc_weekly['Median_tot_hhd_inc_weekly'].apply(
        lambda x: median_range_HI(x))
    collst = df_hhld_inc_weekly.columns.tolist()
    newlst = collst[:2] + ['hhld_weekly_income_median_interval'] + collst[2:-1]
    df_hhld_inc_weekly = df_hhld_inc_weekly[newlst]

    df_overview['hhld_weekly_income_median'] = df_hhld_inc_weekly['Median_tot_hhd_inc_weekly']
    df_overview['hhld_weekly_income_median_interval'] = df_hhld_inc_weekly['hhld_weekly_income_median_interval']
    df_overview['hhld_weekly_income_std'] = df_hhld_inc_weekly['hhld_weekly_income_std']
    df_overview['hhld_weekly_income_std_norm'] = df_hhld_inc_weekly['hhld_weekly_income_std_norm']
    df_overview['hhld_weekly_income_mean'] = df_hhld_inc_weekly['hhld_weekly_income_mean']
    df_overview['hhld_weekly_income_mean_norm'] = df_hhld_inc_weekly['hhld_weekly_income_mean_norm']

    print("Household Income data processed in", (time.time() - start_time), "s\n")
    return(df_overview)


# Occupation
def Load_occupation_data(df_overview):
    start_time = time.time()
    file6 = dir + '2016Census_G57A_AUS_' + granularity + '.csv'
    file7 = dir + '2016Census_G57B_AUS_' + granularity + '.csv'

    df_occupationA = pd.read_csv(file6)
    df_occupationB = pd.read_csv(file7)
    df_occupation = pd.concat([df_occupationA, df_occupationB], axis=1)
    df_occupation = df_occupation.loc[:, ~df_occupation.columns.duplicated()]

    df_occupation['GeoLevel'] = df_occupation[index_code]
    df_occupation = df_occupation.set_index('GeoLevel')
    df_occupation['GeoLevel'] = df_occupation[index_code]

    occu_cols = ['GeoLevel', 'P_Tot_Managers', 'P_Tot_Professionals',
                 'P_Tot_TechnicTrades_W', 'P_Tot_CommunPersnlSvc_W', 'P_Tot_ClericalAdminis_W',
                 'P_Tot_Sales_W', 'P_Tot_Mach_oper_drivers', 'P_Tot_Labourers', 'P_Tot_Occu_ID_NS']

    df_occupation = df_occupation[occu_cols]

    df_overview['occupation_total_Managers'] = df_occupation['P_Tot_Managers']
    df_overview['occupation_total_Professionals'] = df_occupation['P_Tot_Professionals']
    df_overview['occupation_total_TechTradeWorkers'] = df_occupation['P_Tot_TechnicTrades_W']
    df_overview['occupation_total_CommunityPersonalService'] = df_occupation['P_Tot_CommunPersnlSvc_W']
    df_overview['occupation_total_ClericalAdminWorkers'] = df_occupation['P_Tot_ClericalAdminis_W']
    df_overview['occupation_total_SalesWorkers'] = df_occupation['P_Tot_Sales_W']
    df_overview['occupation_total_MachineOperators'] = df_occupation['P_Tot_Mach_oper_drivers']
    df_overview['occupation_total_Labourers'] = df_occupation['P_Tot_Labourers']
    df_overview['occupation_total_NotStated'] = df_occupation['P_Tot_Occu_ID_NS']

    # Population & occupation standardising

    file8 = dir + '2016Census_G01_AUS_' + granularity + '.csv'
    df_population = pd.read_csv(file8)
    df_population['GeoLevel'] = df_population[index_code]
    cols = ['GeoLevel', 'Tot_P_P']
    df_population = df_population[cols]
    n = df_population.shape[0]
    df_population = df_population.sort_values(by=['GeoLevel'])
    df_population[''] = list(range(0, n))
    df_population = df_population.set_index('')

    df_overview = df_overview.sort_index()
    # df_overview['GeoLevel']=df_overview['GeoLevel'].apply(lambda x:int(x))
    df_overview[''] = list(range(0, n))
    df_overview = df_overview.set_index('')
    df_overview = df_overview.sort_values(by=['GeoLevel'])

    df_overview = pd.merge(left=df_population, right=df_overview, how='inner', left_on='GeoLevel', right_on='GeoLevel')
    df_overview = df_overview.rename(columns={'Tot_P_P': 'population'})

    df_overview['occupation_total_Managers_standardised'] = df_overview['occupation_total_Managers']/df_overview['population']
    df_overview['occupation_total_Professionals_standardised'] = df_overview['occupation_total_Professionals']/df_overview['population']
    df_overview['occupation_total_TechTradeWorkers_standardised'] = df_overview['occupation_total_TechTradeWorkers']/df_overview['population']
    df_overview['occupation_total_CommunityPersonalService_standardised'] = df_overview['occupation_total_CommunityPersonalService']/df_overview['population']
    df_overview['occupation_total_ClericalAdminWorkers_standardised'] = df_overview['occupation_total_ClericalAdminWorkers']/df_overview['population']
    df_overview['occupation_total_SalesWorkers_standardised'] = df_overview['occupation_total_SalesWorkers']/df_overview['population']
    df_overview['occupation_total_MachineOperators_standardised'] = df_overview['occupation_total_MachineOperators']/df_overview['population']
    df_overview['occupation_total_Labourers_standardised'] = df_overview['occupation_total_Labourers']/df_overview['population']
    df_overview['occupation_total_NotStated_standardised'] = df_overview['occupation_total_NotStated']/df_overview['population']

    print("Occupation data processed in", (time.time() - start_time), "s\n")
    return(df_overview)
# Employment
def Load_emplyment_data(df_overview):
    start_time = time.time()
    file9 = dir + '2016Census_G40_AUS_' + granularity + '.csv'
    df_edu = pd.read_csv(file9)
    df_edu['GeoLevel'] = df_edu[index_code]

    status_cols = ['GeoLevel', 'lfs_Emplyed_wrked_full_time_P',
                   'lfs_Emplyed_wrked_part_time_P',
                   'lfs_Employed_away_from_work_P',
                   'lfs_Unmplyed_lookng_for_wrk_P',
                   'lfs_Tot_LF_P',
                   'lfs_N_the_labour_force_P',
                   'Percent_Unem_loyment_P',
                   'Percnt_LabForc_prticipation_P',
                   'Percnt_Employment_to_populn_P',
                   'Non_sch_quals_PostGrad_Dgre_P',
                   'Non_sch_quals_Gr_Dip_Gr_Crt_P',
                   'Non_sch_quals_Bchelr_Degree_P',
                   'Non_sch_quls_Advncd_Dip_Dip_P',
                   'Non_sch_quls_Cert3a4_Level_P',
                   'Non_sch_quls_Cert1a2_Level_P',
                   'Non_sch_quls_Certnfd_Level_P',
                   'Non_sch_quls_CertTot_Level_P']

    df_edu = df_edu[status_cols]
    df_edu = df_edu.sort_values(by=['GeoLevel'])
    df_edu['pct_full_time_labourforce'] = df_edu['lfs_Emplyed_wrked_full_time_P'] / df_edu['lfs_Tot_LF_P']
    df_edu['pct_part_time_labourforce_part_time_labourforce'] = df_edu['lfs_Emplyed_wrked_part_time_P'] / df_edu[
        'lfs_Tot_LF_P']
    df_edu['pct_labourforce_looking_for_work'] = df_edu['lfs_Unmplyed_lookng_for_wrk_P'] / df_edu['lfs_Tot_LF_P']
    df_edu = df_edu.fillna(0)

    df_edu['pct_unemployment'] = df_edu['Percent_Unem_loyment_P'].apply(lambda x: x / 100)
    del df_edu['Percent_Unem_loyment_P']
    df_edu['pct_labourforce_participation'] = df_edu['Percnt_LabForc_prticipation_P'].apply(lambda x: x / 100)
    del df_edu['Percnt_LabForc_prticipation_P']
    df_edu['pct_employment_to_population'] = df_edu['Percnt_Employment_to_populn_P'].apply(lambda x: x / 100)
    del df_edu['Percnt_Employment_to_populn_P']

    cols = ['GeoLevel', 'pct_full_time_labourforce',
            'pct_part_time_labourforce_part_time_labourforce', 'pct_labourforce_looking_for_work',
            'pct_unemployment', 'pct_labourforce_participation', 'pct_employment_to_population']
    df_edu = df_edu[cols]

    df_overview = pd.merge(left=df_edu, right=df_overview, how='inner', left_on='GeoLevel', right_on='GeoLevel')

    collst = df_overview.columns.tolist()
    newlst = [collst[0]] + collst[7:] + collst[1:7]
    df_overview = df_overview[newlst]

    df_overview_final = df_overview.rename(columns={'GeoLevel': index_code})
    print("Employment Data processed in", (time.time() - start_time), "s\n")
    return(df_overview_final)

# Missing Vals
def clean_up(df_overview_final):
    to_delete1 = df_overview_final[(df_overview_final['population'] == 0)][index_code].tolist()
    for i in to_delete1:
        df_overview_final = df_overview_final[df_overview_final[index_code] != i]
    return(df_overview_final)



#=======================================================================
#DATA PREPARATION
#=======================================================================
begin_time=time.time()

stage1=Load_wkly_prsnl_inc()
stage2=Load_median_data(stage1)
stage3=Load_hhld_wkly_inc(stage2)
stage4=Load_occupation_data(stage3)
stage5=Load_emplyment_data(stage4)
df_overview_final=clean_up(stage5)

df_overview_final.to_csv(directory+'/features_by_'+granularity+'.csv', index=False)
print("Final dataframe exported after", (time.time() - begin_time), "s")
