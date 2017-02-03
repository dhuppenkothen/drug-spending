import requests # to download the dataset
import zipfile # to extract from archive
import shutil # to write the dataset to file
import os # rename file to something more type-able

import pandas as pd
import numpy as np

import feather

def _download_data(url, data_dir="../data/", data_name="dataset", zipfile=False):
    """
    Helper function to download the data from a given URL into a 
    directory to be specified. If it's a zip file, unzip.

    Parameters
    ----------
    url : string
        String with the URL from where to download the data

    data_dir : string, optional, default: "../data/"
        Path to the directory where to store the new data

    zipfile: bool, optional, default: False
        Is the file we download a zip file? If True, unzip it.
    """

    # figure out if data directory exists
    # if not, create it!
    try:
        os.stat(data_dir)
    except FileNotFoundError:
        os.mkdir(data_dir)

    # open a connection to the URL
    response = requests.get(url, stream=True)

    # store file to disk
    with open(data_dir + data_name, 'wb') as ds_zipout:
        shutil.copyfileobj(response.raw, ds_zipout)

    # if it's a zip file, then unzip:   
    if zipfile:
        zip = zipfile.ZipFile(data_dir + 'dataset', 'r')

        # get list of file names in zip file:
        ds_filenames = zip.namelist()
        # loop through file names and extract each
        for f in ds_filenames:
            zip.extract(f, path=data_dir)

    return 


def download_partd(data_dir="../data/"):

    # URL for the CMS Part D data 
    url = 'https://www.cms.gov/Research-Statistics-Data-and-Systems/'+ \
          'Statistics-Trends-and-Reports/Information-on-Prescription-Drugs/'+ \
          'Downloads/Part_D_All_Drugs_2015.zip'
   
    # download data from CMS:
    _download_data(url, data_dir=data_dir, data_name="part_d.zip", zipfile=True)
     
    # data is in a form of an Excel sheet (because of course it is)
    # we need to make sure we read the right work sheet (i.e. the one with the data):
    xls = pd.ExcelFile(data_dir + "Medicare_Drug_Spending_PartD_All_Drugs_YTD_2015_12_06_2016.xlsx")
    partd = xls.parse('Data', skiprows=3)
    partd.index = np.arange(1, len(partd) + 1)

    # First part: get out the drug names (generic + brand) and store them to a file

    # Capture only the drug names (we'll need this later)
    partd_drugnames = partd.iloc[:, :2]
    partd_drugnames.columns = ['drugname_brand', 'drugname_generic']

    # Strip extraneous whitespace from drug names
    partd_drugnames.loc[:, 'drugname_brand'] = partd_drugnames.loc[:, 'drugname_brand'].map(lambda x: x.strip())
    partd_drugnames.loc[:, 'drugname_generic'] = partd_drugnames.loc[:, 'drugname_generic'].map(lambda x: x.strip())

    # write the results to a feather file:
    feather.write_dataframe(partd_drugnames, data_dir + 'drugnames.feather')

    # Separate column groups by year
    cols_by_year = [
        { 'year': 2011, 'start': 2, 'end': 12 },
        { 'year': 2012, 'start': 12, 'end': 22 },
        { 'year': 2013, 'start': 22, 'end': 32 },
        { 'year': 2014, 'start': 32, 'end': 42 },
        { 'year': 2015, 'start': 42, 'end': 53 },
    ]

    partd_years = {}

    col_brandname = 0
    col_genericname = 1
    for cols in cols_by_year:
        year, start, end = cols['year'], cols['start'], cols['end']

        partd_years[year] = pd.concat([partd_drugnames,
                                       partd.iloc[:, start:end]],
                                      axis=1)

    # Remove 2015's extra column for "Annual Change in Average Cost Per Unit" (we can calculate it, anyhow)
    partd_years[2015] = partd_years[2015].drop(partd_years[2015].columns[-1], axis=1)

    # Drop any rows in each year that have absolutely no data, then reset their row indices
    for year in partd_years:
        nonnull_rows = partd_years[year].iloc[:, 2:].apply(lambda x: x.notnull().any(), axis=1)
        partd_years[year] = partd_years[year][nonnull_rows]
        partd_years[year].index = np.arange(1, len(partd_years[year]) + 1) 

    # Make columns easier to type and more generic w.r.t. year
    generic_columns = [
        "drugname_brand",
        "drugname_generic",
        "claim_count",
        "total_spending",
        "user_count",
        "total_spending_per_user",
        "unit_count",
        "unit_cost_wavg",
        "user_count_non_lowincome",
        "out_of_pocket_avg_non_lowincome",
        "user_count_lowincome",
        "out_of_pocket_avg_lowincome"
    ]

    for year in partd_years:
        partd_years[year].columns = generic_columns

    # Cast all column data to appropriate numeric types

    # Suppress SettingWithCopyWarnings because I think it's
    # tripping on the fact that we have a dict of DataFrames
    pd.options.mode.chained_assignment = None
    for year in partd_years:
        # Ignore the first two columns, which are strings and contain drug names
        for col in partd_years[year].columns[2:]:
            partd_years[year].loc[:, col] = pd.to_numeric(partd_years[year][col])
    pd.options.mode.chained_assignment = 'warn'
 
    # Serialize data for each year to feather file for use in both Python and R
    for year in partd_years:
        feather.write_dataframe(partd_years[year], data_dir + 'spending-' + str(year) + '.feather')

    return
