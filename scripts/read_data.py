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
    """
    Download the Medicare Part D expenditure data from the CMS website.
    This function will dowload the data, load the original Excel file into 
    a pandas DataFrame and do some data wrangling and cleaning. 

    The end result are a file with drug names (both generic and brand) as well 
    as one file per year with the actual data. All output files are `feather` files.

    Parameters
    ----------
    data_dir : string
       The path to the directory where the data should be stored.

    """


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
 
    # make all drugnames lowercase:
    partd_drugnames["drugname_generic"] = partd_drugnames["drugname_generic"].str.lower()
    partd_drugnames["drugname_brand"] = partd_drugnames["drugname_brand"].str.lower()

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

def download_puf(data_dir="../data/", all_columns=True):
    """
    Download the CMS prescription drug profiles.
    This function will dowload the data, load the original CSV file into 
    a pandas DataFrame and do some data wrangling and cleaning. 

    The end result will be a feather file with the prescription drug
    profiles.

    Parameters
    ----------
    data_dir : string
       The path to the directory where the data should be stored.

    all_columns : bool, optional, default: True
       If True, store all columns in a feather file.
       If False, only store the columns with RXCUI ID, drug major class 
       and drug class
    """
    url = "https://www.cms.gov/Research-Statistics-Data-and-Systems/"+\
          "Statistics-Trends-and-Reports/BSAPUFS/Downloads/2010_PD_Profiles_PUF.zip"

    
    # download data from CMS:
    _download_data(url, data_dir=data_dir, data_name="puf.zip", zipfile=True)

    # read CSV into DataFrame
    puf = pd.read_csv("../data/2010_PD_Profiles_PUF.csv")

    # if we don't want to save all columns, drop those except for the three columns 
    # we're interested in.
    if not all_columns:
        puf.drop(["BENE_SEX_IDENT_CD", "BENE_AGE_CAT_CD", "PDE_DRUG_TYPE_CD", "PLAN_TYPE", 
                 "COVERAGE_TYPE", "benefit_phase","DRUG_BENEFIT_TYPE",
                 "PRESCRIBER_TYPE", "GAP_COVERAGE", "TIER_ID", "MEAN_RXHCC_SCORE",
                 "AVE_DAYS_SUPPLY", "AVE_TOT_DRUG_COST", "AVE_PTNT_PAY_AMT",
                 "PDE_CNT", "BENE_CNT_CAT"], axis=1, inplace=True)

    # write to a DataFrame
    feather.write_dataframe(puf, data_dir + 'puf.feather')

    return 

def download_rxnorm(data_dir="../data/"):
    """
    Download RxNorm data for *currently prescribable* drugs. The RxNorm data 
    describes a standard identifier for drugs, along with commonly used names, 
    ingredients and relationships. The full data set is very large and requires a  
    special licence. Here, we use the subset of drugs that can currently be 
    prescribed, which are available without licence. We are also going to ignore 
    the relational data and focus on commonly used identifiers and the RxNorm ID.

    Parameters
    ----------
    data_dir : string
       The path to the directory where the data should be stored.
    """
    # URL to the data file
    url = "https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_01032017.zip"

    # download data from NIH:
    _download_data(url, data_dir=data_dir, data_name="rxnorm.zip", zipfile=True)


    # Column names as copied from the NIH website
    names = ["RXCUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF", "RXAUI",
         "SAUI", "SCUI", "SDUI", "SAB", "TTY", "CODE", "STR", "SRL", "SUPPRESS", "CVF"]

    # we only want column 0 (the RXCUI identifier) and 14 (the commonly used name)
    rxnorm = pd.read_csv("../data/rrf/RXNCONSO.RRF", sep="|", names=names, index_col=False,
                         usecols=[0,14])
 
    # make all strings lowercase
    rxnorm["STR"] = rxnorm["STR"].str.lower()

    # write to a DataFrame
    feather.write_dataframe(rxnorm, data_dir + 'rxnorm.feather')

    return

def download_drug_class_ids(data_dir="../data/"):
    """
    Download the table associating major and minor classes with alphanumeric codes.
    This data originates in the VA's National Drug File, but also exists in more accessible 
    for in the SAS files related to the CMS PUF files.

    Parameters
    ----------
    data_dir : string
       The path to the directory where the data should be stored.

    """

    url = "https://www.cms.gov/Research-Statistics-Data-and-Systems/" + \
          "Statistics-Trends-and-Reports/BSAPUFS/Downloads/2010_PD_Profiles_PUF_DUG.zip"
    
    # download data from CMS:
    _download_data(url, data_dir=data_dir, data_name="drug_classes_dataset.zip", zipfile=True)

    # read drug major classes
    drug_major_class = pd.read_csv(data_dir+"DRUG_MAJOR_CLASS_TABLE.csv")
 
    # read drug minor classes
    drug_class = pd.read_csv(data_dir+"DRUG_CLASS_TABLE.csv")

    # replace NaN values in drug_class table
    drug_class.replace(to_replace=np.nan, value="N/A", inplace=True)

    # write to a DataFrame
    feather.write_dataframe(drug_major_class, data_dir + 'drug_major_class.feather')
    feather.write_dataframe(drug_class, data_dir + 'drug_class.feather')

    return

def make_drug_table(data_dir="../data/", data_local=True):
    """ 
    Make a table that associates:
        * drug brand name
        * drug generic name
        * drug RxNorm RXCUI Identifier
        * drug major class
        * drug minor class

    If the data doesn't exist locally, it will be downloaded.
    The output is a feather file called `drugnames_withclasses.feather`.

    Parameters
    ----------
    data_dir : string, optional, default: "../data/"
        The directory that contains the data as .feather files.

    data_local : bool, optional, default: True
        If True, code assumes that the data exists locally. If this is not 
        the case, the function will exit with an error. If False, data will  
        be downloaded to the directory specified in `data_dir`. 

    """ 
    # if data_local is False, download all the necessary data
    download_partd(data_dir)
    download_puf(data_dir, all_columns=False)
    download_rxnorm(data_dir)
    download_drug_class_ids(data_dir)

    # assert that data directory and all necessary files exist.
    assert os.path.isdir(data_dir), "Data directory does not exist!"
    assert os.path.isfile(data_dir+"drugnames.feather"), "Drugnames file does not exist!"
    assert os.path.isfile(data_dir+"puf.feather"), "Prescription drug profile data file does not exist!"
    assert os.path.isfile(data_dir+"rxnorm.feather"), "RxNorm data file does not exist!"
    assert os.path.isfile(data_dir+"drug_major_class.feather"), "Drug major class file does not exist."
    assert os.path.isfile(data_dir+"drug_class.feather"), "Drug class file does not exist."

    # load data files from disk
    drugnames = feather.read_dataframe(data_dir + "drugnames.feather")
    puf = feather.read_dataframe(data_dir + "puf.feather")
    rxnorm = feather.read_dataframe(data_dir + "rxnorm.feather")
    drug_major_class = feather.read_dataframe(data_dir + "drug_major_class.feather")
    drug_class = feather.read_dataframe(data_dir + "drug_class.feather")

    # make a new column for RXCUI values
    drugnames["RXCUI"] = "0.0"    

    # associate drug names with RXCUI codes
    # NOTE: THIS IS A BIT HACKY! 
 
    # loop over indices in list of drug names
    for idx in drugnames.index:

        # sometimes, we might have more than one RXCUI 
        # associated with a drug, because the names can 
        # be a bit ambivalent, so make a list
        rxcui = []

        # we are going to look for RXCUI codes for both the 
        # generic name of the drug and the brand name of the drug
        # because sometimes one might be associated and the other
        # one isn't
        for c in ["drugname_generic", "drugname_brand"]:

            # get out the correct row in the table
            d = drugnames.loc[idx, c]
            # sometimes a drug has two names, split by a slash
            # we are going to try and find RXCUI codes for both
            dsplit = d.split("/")

            # loop over drug names
            for di in dsplit:
                # sometimes, a drug has a suffix attached to it
                # since this doesn't usually exist in the RxNorm table,
                # we strip anything after a free space
                displit = di.split(" ")
                v = rxnorm[rxnorm["STR"] == displit[0]]

                # include all unique RXCUI codes in the list
                if len(v) > 0:
                    rxcui.extend(v["RXCUI"].unique())
                else:
                    continue

        # if there are more than one RXCUI identifier for a drug,
        # make a string containing all codes, separated by a '|'
        if len(rxcui) > 1:
            rxcui_str = "|".join(np.array(rxcui, dtype=str))

        elif len(rxcui) == 1:
            rxcui_str = str(rxcui[0])
        else:
            # if there is no RXCUI code associated, include a 0
            rxcui_str = '0.0'

        # associate string with RXCUI codes with the correct row
        drugnames.loc[idx, "RXCUI"] = rxcui_str

    # number of drugs that I can't find RXCUI codes for:
    n_missing = len(drugnames[drugnames["RXCUI"] == '0.0'])
 
    print("A fraction of %.2f drugs has no RxNorm entry that I can find."%(n_missing/len(drugnames)))

    # add empty columns for drug classes and associated strings
    drugnames["drug_major_class"] = ""
    drugnames["dmc_string"] = ""
    drugnames["drug_class"] = ""
    drugnames["dc_string"] = ""

    # make sure RXCUI codes are all strings:
    drugnames["RXCUI"] = drugnames["RXCUI"].astype(str)

    # loop over drug names again
    for idx in drugnames.index:

        # get out the RxCUI codes for this entry
        drug_rxcui = drugnames.loc[idx, "RXCUI"].split("|")
        
        # the same way that one drug may have multiple RXCUI codes,
        # it may also have multiple classes, so make an empty list for them
        dmc, dc = [], []

        # if there are multiple RXCUIs, we'll need to loop over them:
        for rxcui in drug_rxcui:
            # find the right entry in the prescription drug profile data for 
            # this RXCUI
            r = puf[puf["RXNORM_RXCUI"] == np.float(rxcui)]

            # there will be duplicates, so let's pick only the set of unique IDs
            rxc = r.loc[r.index, "RXNORM_RXCUI"].unique()

            # add drug classes for this RXCUI to list
            dmc.extend(r.loc[r.index, "DRUG_MAJOR_CLASS"].unique())
            dc.extend(r.loc[r.index, "DRUG_CLASS"].unique())

        # multiple RXCUIs might have the same class, and we only care
        # about unique entires
        dmc = np.unique(dmc)
        dc = np.unique(dc)

        # if there is at least one drug class associated with the drug, 
        # make a string of all associated drug classes separated by `|`
        # and store in correct row and column
        if len(dmc) != 0:
            drugnames.loc[idx, "drug_major_class"] = "|".join(dmc)
            dmc_name = np.hstack([drug_major_class.loc[drug_major_class["drug_major_class"] == d, 
                                                       "drug_major_class_desc"].values for d in dmc])
            drugnames.loc[idx, "dmc_name"] = "|".join(dmc_name)

        # if there is no class associated, this entry will be zero
        else:
            drugnames.loc[idx, "drug_major_class"] = "0"
            drugnames.loc[idx, "dmc_name"] = "0"

        # same procedure as if-statement just above
        if len(dc) != 0:
            drugnames.loc[idx, "drug_class"] = "|".join(dc)
            dc_name = np.hstack([drug_class.loc[drug_class["drug_class"] == d, 
                                                "drug_class_desc"].values for d in dc])   

            drugnames.loc[idx, "dc_name"] = "|".join(dc_name)
        else:
            drugnames.loc[idx, "drug_class"] = "0"
            drugnames.loc[idx, "dc_name"] = "0"

    # write results to file
    feather.write_dataframe(drugnames, data_dir + 'drugnames_withclasses.feather')
   
    return




