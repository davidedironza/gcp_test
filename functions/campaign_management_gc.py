# -*- coding: utf-8 -*-
import os
import re
import sys
import math
import pickle
import getpass 
import datetime
import operator
import traceback 
import numpy  as np
import pandas as pd


from scipy                     import stats as sc
from random                    import random, randint
from numpy                     import NaN
#from openpyxl                  import load_workbook
#from openpyxl.styles           import PatternFill
#from openpyxl.styles.borders   import Border, Side
#from openpyxl.styles.alignment import Alignment

#import CCDA_Standard_Functions as sf

import importlib
import soa_gc as so
importlib.reload(so)


#%% Campaign: Beinhaltet grundlegende Kampagnen-Informationen & Stati
class Campaign(object):
    def __init__(self,
                 campaign_id,
                 campaign_name, 
                 campaign_manager,
                 campaign_techsupport,
                 campaign_startdate,
                 campaign_enddate,
                 campaign_pathdata,
                 campaign_lineofbusiness  = "NL",                 
                 campaign_sharekg         = "Permanent",
                 campaign_channelsplit    = {"AD": 1.0},                   
                 campaign_channelsplitvar = None,
                 campaign_trackausschluss = False, 
                 campaign_initialize      = False):

        """Creates new campaign object, holding all relevant information for campaign management.
        Controls productive operation of campaigns. Campaign information is inherited to tranche objects. 
    
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        campaign_id              << int >>
                                    ID of campaign (e.g., 12345)
        campaign_name            << str >>
                                    Name of campaign
        campaign_manager         << list >>
                                    List of strings with email addresses of campaign managers to be notified
                                    of campaign run success & failure in production (PROD)                                 
        campaign_techsupport     << list >>
                                    list of strings with email addresses of technical staff (Data Scientists
                                    to be notified of campaign run success & failure in production (PROD)
                                    and during testing (DEV, ACC)
        campaign_startdate       << str >> or << datetime.datetime >>
                                    Start date of the campaign (e.g., '12.03.2017'). Makes sure a scheduled campaign
                                    starts no earlier than the set date and sets the initial timelastrun of a trigger-
                                    based campaign
        campaign_enddate         << str >> or << datetime.datetime >>
                                    End date of the campaign (e.g., '31.12.2017'). Scheduled campaigns stop once the
                                    current date exceeds the end date
        campaign_pathdata        << str >>
                                    Path where campaign backup is stored and the file 'campaign_exclusions.csv' is stored
                                    (e.g. T:/CCDA/01_DBM/01_Libraries/03_KAMPAGNEN/2017/K10217_NAM_IL/Data).
        campaign_lineofbusiness  << str >>
                                    Indicates whether campaign has life ("L") or non-life ("NL") focus.
                                    Used for assigning the correct CRM Vertragsbetreuer.                                       
        campaign_sharekg         << str (optional) >> or << float >>
                                    Defines how control group of campaign is set. Set 'Permanent' to use Permanent Control
                                    Group of SAP Hybris. Set a float to set a random control group share (e.g., 0.1 for 10%)
        campaign_channelsplit    << dict (optional) >>
                                    Controls the channel mix for the campaign. The following modes are supported, which you
                                    specify by providing a corresponding dict object (and set campaign_channelsplitvar):
                                    a) Percentage distribution of leads over channels
                                       Set the share of leads each channel gets. Ensure that dict values add up to 1.
                                       Use dict of the form: 
                                           {"AD": 0.5, "TELEFON": 0.3, "EMAIL": 0.2, "BRIEF": 0.1}
                                    b) Absolute distribution of leads over channels
                                       Set the maximum number of leads per channel for a tranche run. Set None if no capacity
                                       constraints apply. If channel is omitted in the dict, no leads will go to this channel.
                                       Use dict of the form:
                                           {"AD": None, "TELEFON": 300}
                                    c) Percentage distribution of leads over channels, individual by custom variable
                                       Set the share of leads each channel gets for each category of a custom variable.
                                       Allows to set different channel mixes e.g. for different distribution units of a 
                                       custom variable (specified in campaign_channelsplitvar). Make sure that each category
                                       of the custom variable is in the keys of the dict campaign_channelsplit. Ensure that 
                                       values of individual channel dicts each add up to 1.
                                       Use dict of the form:
                                           {"GA Winterthur": {"AD": 0.5, "TELEFON": 0.3, "EMAIL": 0.2, "BRIEF": 0.1}, 
                                            "GA Wil":        {"AD": 0.8, "TELEFON": 0.1, "EMAIL": 0.1, "BRIEF": 0.0}}
                                    d) Absolute distribution of leads over channels, individual by custom variable
                                       Set the maximum number of leads per channel for each category of a custom variable.
                                       Allows to set different channel mixes e.g. for different distribution units of a 
                                       custom variable (specified in campaign_channelsplitvar). Make sure that each category
                                       of the custom variable is in the keys of the dict campaign_channelsplit. 
                                       Use dict of the form:
                                           {"GA Winterthur": {"AD": 300, "TELEFON": 800, "EMAIL": 200, "BRIEF": 0}, 
                                            "GA Wil":        {"AD": 0, "TELEFON": None, "EMAIL": 150, "BRIEF": 300}}                                            
        campaign_channelsplitvar << str (optional) >>
                                    Specifies custom variable over which to distinguish channel mix. Required for modes c)
                                    and d) of campaign_channelsplit
        campaign_trackausschluss << bool (optional) >>
                                    Defines how to handle leads with channel "AUSSCHLUSS", which are not forwarded to one
                                    of the channels AD, TELEFON, EMAIL, and BRIEF. For trigger-based campaigns (with 
                                    customer events), it may be desired to keep track of customers which are not approached
                                    by one of the channels, e.g. for retraining affinity models. In convertional campaigns
                                    (spread over time), customers not considered for a specific tranche are less important
                                    to track (as they will be approached in a next tranche eventually).
                                    Set True to perform the following actions on leads with channel "AUSSCHLUSS": 
                                        a) Write them to the backup file
                                        b) Write them to non_print_sel
                                        c) Report on them in the channel report
                                    Set False to not do any of the above
        campaign_initialize      << bool (optional) >>
                                    Indicates whether campaign object should be initialized with the provided information 
                                    or whether the campaign object should be loaded from backup (pickle). Set True if you 
                                    want to overwrite an existing backup file, otherwise keep set False.

        Return Value:
        None
        """



        #Assertions
        assert(type(campaign_manager) == list), \
               "Please profile campaign manager info as list of eMail addresses"
        assert(type(campaign_techsupport) == list), \
               "Please profile tech support info as list of eMail addresses"
        assert(type(campaign_channelsplit) == dict), \
               "Please use dict to specify channel split / channel capacity constraints"
        assert(campaign_lineofbusiness in ["L", "NL"]), \
               "Please use 'L' (life) or 'NL' (non-life) to denominate the line of business for the campaign"               
               
        assertstring = "Please set campaign_sharekg = 'Permanent' to apply the permanent control group or set between 0 and 1"
        if type(campaign_sharekg) == str: assert(campaign_sharekg == "Permanent"), assertstring      
        elif type(campaign_sharekg) in (int, float): assert(0 <= campaign_sharekg <= 1), assertstring
        else: raise AssertionError(assertstring)       
        
        #Initialize General Information
        self.campaign_id             = str(campaign_id)
        self.campaign_name           = campaign_name
        self.campaign_manager        = campaign_manager 
        self.campaign_techsupport    = campaign_techsupport
        self.campaign_lineofbusiness = campaign_lineofbusiness
        
        #Initialize Date Information
        def init_date(x):
            if type(x) == str:
                date_regex = re.compile('\d{2}[.]\d{2}[.]\d{4}')
                if date_regex.match(x):
                    return datetime.datetime.strptime(x, "%d.%m.%Y")
                else:
                    raise AssertionError("No correct date format dd.mm.yyyy provided.")
            elif type(x) == datetime.datetime: return x
            else:   
                raise AssertionError("Date format not supported. Use str (dd.mm.yyyy) or datetime.datetime")
                return 
        
        self.campaign_startdate = init_date(campaign_startdate)
        self.campaign_enddate   = init_date(campaign_enddate)   

        assert(self.campaign_enddate >= datetime.datetime.combine(datetime.date.today(), datetime.time(0))), \
               "Campaign expired. Please check campaign end date"
                
        #Initialize Control Group, Tranche & Channel Information
        self.campaign_channellist = ['AD', 'TELEFON', 'BRIEF', 'EMAIL']
        
        def share_checker(input_dict):
            aux_dict =  {i:input_dict[i] for i in input_dict if input_dict[i] != None}    
        
            if (round(sum(aux_dict.values()), 3) == 1) and all([(0 <= i <= 1) for i in aux_dict.values()]): return True
            elif all([type(i) == int for i in aux_dict.values()]):                                return False
            else: raise AssertionError("Please provide all int or all float (with sum = 1) for values of dict(s) on lowest level")                
            return      
            
        if all([str(i).upper() in self.campaign_channellist for i in campaign_channelsplit.keys()]):
            assert(campaign_channelsplitvar == None), \
            "Please set campaign_channelsplitvar = None"     
            
            key_is_channel = True        
            value_is_share = share_checker(campaign_channelsplit)
           
        else:                                                                                                                    
            assert(campaign_channelsplitvar != None), \
            "Please provide campaign_channelsplitvar, indicating by which attribute channel assignment is grouped"            
            assert(all([type(i) == dict for i in campaign_channelsplit.values()])), \
            "Please provide all dicts of the form {'AD': 500, 'TELEFON': 300} or {'AD': 0.6, 'TELEFON': 0.4} as values"
            assert(all([all([j in self.campaign_channellist for j in i.keys()])for i in campaign_channelsplit.values()])), \
            "Please provide only 'AD', 'TELEFON', 'BRIEF', 'EMAIL' for all keys of the dicts"
            
            key_is_channel = False
            if   all([    share_checker(i) for i in campaign_channelsplit.values()]): value_is_share = True
            elif all([not share_checker(i) for i in campaign_channelsplit.values()]): value_is_share = False
            else: raise AssertionError("Please provide either all int or all float at dict(s) on lowest level")    
               
        self.campaign_sharekg         = campaign_sharekg
        self.campaign_channelsplit    = campaign_channelsplit
        self.campaign_channelsplitvar = campaign_channelsplitvar
        self.campaign_channelmode     = {"key_is_channel": key_is_channel, "value_is_share": value_is_share}
        self.campaign_trackausschluss = campaign_trackausschluss
        
        #Initialize Technical Information & Backup
        self.campaign_pathdata = campaign_pathdata           
                 
        try: 
            aux_kampagne = pickle.load(open(campaign_pathdata + "kampagne.pkl", 'rb'))
        except FileNotFoundError:
            campaign_initialize = True
       
        if campaign_initialize: 
            self.campaign_backup      = pd.DataFrame(columns = ["TRANCHE_NUMMER", "TRANCHE_DATUM", "TRANCHE_KG", "PART_NR"])
            self.campaign_tranche     = 0    
            self.campaign_timelastrun = self.campaign_startdate      
        else:     
            self.campaign_backup      = aux_kampagne.campaign_backup
            self.campaign_tranche     = aux_kampagne.campaign_tranche
            self.campaign_timelastrun = aux_kampagne.campaign_timelastrun
                 
        #Initialize Campaign Exclusion Dictionary (selectively use campaigns for de-selection)
        try: 
            self.campaign_exclusions = pd.read_csv(self.campaign_pathdata + "campaign_exclusions.csv",sep = ";")
            self.campaign_exclusions = dict(zip(self.campaign_exclusions.KMPGN_ID.astype(str), 
                                                self.campaign_exclusions.USE_FOR_DESELECTION))
        except:
            self.campaign_exclusions = None
                                  
    def backup(self, tranche, var_list):
        """Writes campaign information to backup and completes the tranche production run (i.e. counting up tranche
        number and updating the campaign.campaign_timelastrun with the time the tranche was run). The backup includes 
        all selected customers (target & control group) and their attributes (specified by var_list). 
        The backup will be written into the path defined by campaign.campaign_pathdata.
        
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        tranche           << tranche object >>
                             Tranche object whose selected customers (target & control group) will be added to the backup
        var_list          << list >>
                             List of str with the attribute names that shall be backed up.
                             Best practice is to back up at least the following: 
                             ["TRANCHE_NUMMER", "TRANCHE_DATUM", "TRANCHE_KG", "PART_NR", "KANAL"]

        Return Value:
        None
        """
        
        if tranche.stage == "PROD":
            #Update Backup-DataFrame
            if self.campaign_trackausschluss == False: 
                self.campaign_backup = self.campaign_backup.append(tranche.tranche_df[tranche.tranche_df.KANAL != "AUSSCHLUSS"][var_list])
            else:
                self.campaign_backup = self.campaign_backup.append(tranche.tranche_df[var_list])
            self.campaign_backup = self.campaign_backup.reset_index(drop = True)

            #Update Tranche Number
            self.campaign_tranche += 1
            
            #Update Time Last Run
            self.campaign_timelastrun = tranche.tranche_timestamp
            
            #Saving Campaign-Element to Disk
            pickle.dump(self, open(self.campaign_pathdata + "kampagne.pkl", 'wb'))


    
#%% Trache: Beinhaltet grundlegende Tranchen-Informationen & selektierte Kundendaten 
class Tranche(object):
    def __init__(self, campaign, df_tranche, stage):
        """Creates new tranche object. It is initialized by inheriting information from a campaign object 
        and getting a DataFrame with customer data from the campaign's base selection. 
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        campaign   << campaign object >>
                      Campaign object from which the tranche object inherits campaign parameters
        df_tranche << pd.DataFrame >> or << pd.Series >>
                      DataFrame / Series object with customers of base selection. Please note that at least the
                      column 'part_nr' / 'PART_NR' is required
        stage      << bool >>
                      Indicates the current staging level (DEV, ACC, or PROD). If in DEV or ACC, 
                      no leads will be forwarded into the channels and no backup processes will be performed 

        Return Value:
        None
        """


        
        #Assertions
        assert(type(df_tranche) in [pd.core.frame.DataFrame, pd.core.series.Series]), \
        "Please provide customers as pd.DataFrame or pd.Series"

        #Initialize Campaign Information
        self.campaign_id                  = campaign.campaign_id
        self.campaign_name                = campaign.campaign_name
        self.campaign_manager             = campaign.campaign_manager 
        self.campaign_techsupport         = campaign.campaign_techsupport
        self.campaign_lineofbusiness      = campaign.campaign_lineofbusiness
        
        #Initialize Tranche & Channel Information
        self.tranche_number               = campaign.campaign_tranche + 1
        self.tranche_pathdata             = campaign.campaign_pathdata
        self.tranche_sharekg              = campaign.campaign_sharekg
        self.tranche_channelsplit         = campaign.campaign_channelsplit
        self.tranche_channelsplitvar      = campaign.campaign_channelsplitvar
        self.tranche_channelmode          = campaign.campaign_channelmode
        self.tranche_channellist          = campaign.campaign_channellist
        self.tranche_trackausschluss      = campaign.campaign_trackausschluss
        
        #Initialize Timestamp & Status
        self.tranche_timestamp            = datetime.datetime.now()
        self.tranche_status               = "1: New"
        
        #Initialize Tranche DataFrame
        self.tranche_df                   = pd.DataFrame(df_tranche)
        self.tranche_df["TRANCHE_DATUM"]  = self.tranche_timestamp
        self.tranche_df["TRANCHE_NUMMER"] = self.tranche_number
        
        #Initialize Tranche KPIs & Hybris Report
        self.kpi_customers_identified     = df_tranche.shape[0]
        self.kpi_customers_selected       = None
        self.kpi_size_targetgroup         = None
        self.kpi_size_controlgroup        = None
        self.kpi_reports                  = []
        self.kpi_hybris                   = None
        
        #Stage Tranche
        self.stage                        = stage
        return 
    
    def update_df(self, df_tranche):
        """Provides an interface to update the DataFrame within a tranche. 
        Automatically handles updating the relevant KPIs of the tranche. 
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        df_tranche << pd.DataFrame >> or << pd.Series >>
                      DataFrame / Series object with customers of base selection. Please note that at least the
                      column 'part_nr' / 'PART_NR' is required

        Return Value:
        None
        """

        #Assertions
        assert(type(df_tranche) in [pd.core.frame.DataFrame, pd.core.series.Series]), \
        "Please provide customers as pd.DataFrame or pd.Series"
        assert(self.tranche_status in ["1: New", "2: Deselected", "3: Prepared for routing"]), \
        "This operation can only be performed at the following campaign stages: '1: New', '2: Deselected', '3: Prepared for routing'"        

        #Update Tranche
        self.tranche_df                   = df_tranche
        self.tranche_df["TRANCHE_DATUM"]  = self.tranche_timestamp
        self.tranche_df["TRANCHE_NUMMER"] = self.tranche_number
        
        #Update Tranche KPIs 
        if   self.tranche_status == "1: New":        self.kpi_customers_identified = df_tranche.shape[0]
        elif self.tranche_status == "2: Deselected": self.kpi_customers_selected   = df_tranche.shape[0]
        elif self.tranche_status == "3: Prepared for routing":
            self.kpi_size_targetgroup  = df_tranche[(df_tranche.TRANCHE_KG == 0) & (df_tranche.KANAL != "AUSSCHLUSS")].shape[0]
            self.kpi_size_controlgroup = df_tranche[(df_tranche.TRANCHE_KG == 1) & (df_tranche.KANAL != "AUSSCHLUSS")].shape[0]
    
    def assign_channel(self, connection_tdb, score_variable = None, score_threshold = None):
        """Automatically assign a channel to different leads based on the specifications of 
        campaign.campaign_channelsplit and campaign.campaign_channelsplitvar.
        Applied channels are "AD", "TELEFON", "BRIEF", "EMAIL", and "AUSSCHLUSS". 
        "AUSSCHLUSS" indicates that a lead is not distributed to any channel. The channel information 
        is added to the underlying DataFrame of the tranche at tranche.tranche_df.KANAL.
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        connection_tdb  << sqlalchemy.engine.base.Engine >> or << sqlalchemy.engine.base.Connection >>
                           Connection as defined by sql_connection()                          
        score_variable  << str >>
                           Indicates variable holding score values used to indicate leads which are
                           distributed to different channels. 
        score_threshold << float >>
                           Minimum score which leads need to have to be considered for channel split.
                           Leads with a score below that threshold are excluded ("AUSSCHLUSS").
                           Use float values between 0 and 1

        Return Value:
        None
        """        
        ##################################################
        # Data Enrichment                                #
        ##################################################
        #Assertions
        assert(int(self.tranche_status[0]) == 2), \
        "Routing not applied in correct process phase. First run customer de-selection(s)"
        
        if self.tranche_df.shape[0] == 0: 
            self.report_emptydf()
        
        #Add Standard Customer Information from CV DirectSales 
        org_list     = ["KDBTR_{0}_PART_NR",        "KDBTR_{0}_CNUMMER",        "KDBTR_{0}_VORNAME",
                        "KDBTR_{0}_NACHNAME",       "KDBTR_{0}_EMAIL",          "KDBTR_{0}_DE_VPAGT",
                        "KDBTR_{0}_PDI_NR",         "KDBTR_{0}_DE_GA_CODE",     "KDBTR_{0}_DE_ASHRF",       
                        "KDBTR_{0}_DE_NAME",        "KDBTR_{0}_DE_NAME_ZUS",    "KDBTR_{0}_DE_STR",            
                        "KDBTR_{0}_DE_PLZ",         "KDBTR_{0}_DE_ORT",         "KDBTR_{0}_DE_TELNR",       
                        "KDBTR_{0}_DE_EMAIL"]
        
        contact_list = ["ADR_MOBILE_1_PRIVAT",      "ADR_MOBILE_2_PRIVAT",      "ADR_MOBILE_3_PRIVAT",
                        "ADR_MOBILE_1_GSCHFTL",     "ADR_MOBILE_2_GSCHFTL",     "ADR_MOBILE_3_GSCHFTL",
                        "ADR_TELEFON_1_PRIVAT",     "ADR_TELEFON_2_PRIVAT",     "ADR_TELEFON_3_PRIVAT",
                        "ADR_TELEFON_1_GSCHFTL",    "ADR_TELEFON_2_GSCHFTL",    "ADR_TELEFON_3_GSCHFTL",
                        "ADR_EMAIL_1_PRIVAT",       "ADR_EMAIL_1_GSCHFTL"]
                    
        var_list     = ["ADR_ANSCHRIFT",            "ADR_VORNAME",              "ADR_NACHNAME",             
                        "ADR_PLZ",                  "ADR_STRASSE",              "ADR_HAUSNUMMER",                    
                        "ADR_POSTFACH_NR",          "ADR_ORT",                  "ADR_LANDESTEIL",           
                        "PED_GESCHLECHT_CODE",      "PED_ZIVILSTAND",           "PED_SPRACHE",              
                        "PED_NATIONALITAT_CODE",    "PED_GEBURTSTAG",           "HAS_TEL",    
                        "HAS_EMAIL"] + \
                       [i.format("NL") for i in org_list] + \
                       [i.format("L")  for i in org_list]
                                                     
        df = self.tranche_df.drop([i for i in self.tranche_df.columns if i in var_list], axis = 1)

        df = sf.sql_leftjoin(df = df, connection = connection_tdb, \
                             schema = "fbtdbmk", tablename = "nv_customer_view_directsales_t", key = "PART_NR",
                             attributes = contact_list + var_list, column_lower = False)

        #Denominate leading CRM Vertragsbetreuer information (and fill missing with Betreuer of other LOB)
        aux_dict = {"NL": "L", "L": "NL"}
        for i in org_list: 
            df[i.format("MAIN")] = df[i.format(self.campaign_lineofbusiness)]
            aux_index = df[df[i.format("MAIN")].isnull()].index
            df.loc[aux_index, i.format("MAIN")] = df[i.format(aux_dict[self.campaign_lineofbusiness])]
        
        df = df.drop([i.format("NL") for i in org_list] + [i.format("L") for i in org_list], axis = 1)

        #Data Preparation & Priorization of Adfress Information (Main eMail Address & 5 Telephone Numbers)        
        df["PED_GEBURTSTAG"] = df.PED_GEBURTSTAG.apply(lambda x: NaN if x == None else x).astype('datetime64[ns]')
        df["PED_SPRACHE"]    = df.PED_SPRACHE.fillna("DE")
        #df["KDBTR_MAIN_DE_NAME_KURZ"] = df.KDBTR_MAIN_DE_NAME_KURZ.fillna("n/a").apply(lambda x: re.sub(r'\d+', '', x).rstrip())
           
        for i in contact_list: df[i].fillna("", inplace = True)
        df["EMAIL_ADR"] = df.apply(lambda row: row.ADR_EMAIL_1_PRIVAT if row.ADR_EMAIL_1_PRIVAT != "" else row.ADR_EMAIL_1_GSCHFTL, axis=1)
        df["AUX_COL"] = df[contact_list[:-2]].apply(lambda x: [i for i in x if i != ''], axis=1)
        
        for i in range(5): 
            df["TEL_NR_" + str(i+1)] = df.AUX_COL.apply(lambda x: x[i] if len(x) >= i+1 else "")

            
        ##################################################
        # Channel Assignment                             #
        ##################################################            
            
        def channel_assigner(df, index, size):
            if index == None: aux_dict = self.tranche_channelsplit
            else:             aux_dict = self.tranche_channelsplit[index]
            aux_dict = dict(zip([i for i in aux_dict.keys()], [i if i != None else df.shape[0] for i in aux_dict.values()]))
                
            aux_index = df[(df.KANAL == "") & (df.HAS_TEL == 1)][:round(size * aux_dict.get("TELEFON", 0))].index   
            df.loc[aux_index, "KANAL"] = "TELEFON"
            
            aux_index = df[(df.KANAL == "") & (df.HAS_EMAIL == 1)][:round(size * aux_dict.get("EMAIL", 0))].index   
            df.loc[aux_index, "KANAL"] = "EMAIL"                  
        
            aux_index = df[(df.KANAL == "")][:round(size * aux_dict.get("BRIEF", 0))].index   
            df.loc[aux_index, "KANAL"] = "BRIEF"            
   
            aux_index = df[(df.KANAL == "")][:math.ceil(size * aux_dict.get("AD", 0))].index   
            df.loc[aux_index, "KANAL"] = "AD"             
            return df
                
        key_is_channel = self.tranche_channelmode["key_is_channel"]
        value_is_share = self.tranche_channelmode["value_is_share"]
        
        df["RANDOM"] = df.PART_NR.apply(lambda x: random())
        df["KANAL"]  = ""
        df = df.sort_values("RANDOM").reset_index(drop = True)
        
        if (score_variable != None) or (score_threshold != None):
            assert(type(score_variable) == str), "Please provide a str fpr score_threshold"
            assert(score_variable in df.columns.values), "Variable score_threshold not in DataFrame of tranche"        
            assert(type(score_threshold) in (int, float) and (0 <= score_threshold <= 1)), \
            "Please set score_threshold a float between 0 and 1"
        
            aux_index = df[df[score_variable] < score_threshold].index
            df.loc[aux_index, "KANAL"] = "AUSSCHLUSS" 
                
        if key_is_channel:
            if     value_is_share: df = channel_assigner(df, None, df.shape[0])
            if not value_is_share: df = channel_assigner(df, None, 1)
                              
        if not key_is_channel:
            assert(set(self.tranche_channelsplit.keys()) >= set(df[self.tranche_channelsplitvar].unique())), \
            "Not all values of {0} provided as keys of campaign_channelsplit".format(self.tranche_channelsplitvar)
            
            result_df = pd.DataFrame()
            
            for i in self.tranche_channelsplit.keys():                
                aux_df = df[df[self.tranche_channelsplitvar] == i].copy()

                if     value_is_share: result_df = result_df.append(channel_assigner(aux_df, i, aux_df.shape[0]))
                if not value_is_share: result_df = result_df.append(channel_assigner(aux_df, i, 1))
                
            df = result_df.reset_index(drop = True).copy()
            
        df["KANAL"] = df.KANAL.apply(lambda x: "AUSSCHLUSS" if x == "" else x)
                
        #Bildung KG
        df["RANDOM"] = df.PART_NR.apply(lambda x: random())
        df = df.sort_values("RANDOM").reset_index(drop = True)
        
        if self.tranche_sharekg != "Permanent": 
            df["TRANCHE_KG"] = (df.index < df.shape[0] * self.tranche_sharekg).astype(int)
        else:   
            if self.stage == "ACC": con_hana = sf.sql_connection("hana_acc", user = 'H905226', pw = sf.get_password("hana_acc", 'H905226'))           
            else:                   con_hana = sf.sql_connection("hana_prod", user = 'H905226', pw = sf.get_password("hana_prod", 'H905226'))           
            sql_statement = """select distinct(PART_NR_INT4) as PART_NR from "_SYS_BIC"."axach.operativ.segm.seg_bol/CA_PART_AUSSCHL_PERMNT_KG" """           
            aux_df = sf.sql_getdf(sql_statement, con_hana, column_lower = False)
            df["TRANCHE_KG"] = df.PART_NR.apply(lambda x: 1 if x in aux_df.PART_NR.unique() else 0)
 
        #Generierung Attribute Formatvorlage DirectSales
        var_list = ['KMPGN_NAME', 'KMPGN_ID', 'SOURCE_KEY_ID', 'PART_NR', 'PART_PERS_ID', 'TEL_NR_KOMPL_01', 'TEL_NR_KOMPL_02', 'TEL_NR_KOMPL_03', 'TEL_NR_KOMPL_04', \
                    'TEL_NR_KOMPL_05', 'ANREDE_BEZ', 'TITEL_BEZ', 'VNAME', 'NNAME', 'STR_NAME', 'HAUS_NR_KOMPL', 'POFA_NR', 'PLZ', 'ORT_NAME', 'KT_CDU', 'ZIVST_BEZ', \
                    'SPRA_CDI', 'NATT_CDI', 'EMAIL_ADR', 'GEB_DAT', 'ALTER_NP', 'AGT_BEZ', 'GAGT_BEZ', 'BRTR_NAME_KOMPL', 'USER_ID_BRTR', 'KZ_ZEILE_AGGRG', \
                    'KZ_ZEILE_WEIT', 'KMPGN-ANTW-ID', 'POL_NR01', 'POL_NR02', 'POL_NR03', 'POL_NR04', 'POL_NR05', 'IENET_RGSTRG_CD'] + \
                   ['TXT_ATSH_VARBLE_' + str(i).zfill(2) for i in range(1,41)] + ['KMPGN_KNTKT_STAT_BEZ']
                  
        for i in [j for j in var_list if j not in ["PART_NR", "EMAIL_ADR"]]: df[i] = ""

        df["KMPGN_NAME"] = self.campaign_name
        df["KMPGN_ID"] = self.campaign_id
        df["SOURCE_KEY_ID"] = self.campaign_id + "-TR" + str(self.tranche_number).zfill(4) + "B-EK-CT"
        df["PART_PERS_ID"] = 0
        df["TEL_NR_KOMPL_01"] = df.TEL_NR_1
        df["TEL_NR_KOMPL_02"] = df.TEL_NR_2
        df["TEL_NR_KOMPL_03"] = df.TEL_NR_3
        df["TEL_NR_KOMPL_04"] = df.TEL_NR_4
        df["TEL_NR_KOMPL_05"] = df.TEL_NR_5
        df["ANREDE_BEZ"] = df.PED_GESCHLECHT_CODE.map({1: "Herr", 2: "Frau"})
        df["VNAME"] = df.ADR_VORNAME
        df["NNAME"] = df.ADR_NACHNAME      
        df["STR_NAME"] = df.ADR_STRASSE
        df["HAUS_NR_KOMPL"] = df.ADR_HAUSNUMMER
        df["POFA_NR"] = df.ADR_POSTFACH_NR
        df["PLZ"] = df.ADR_PLZ
        df["ORT_NAME"] = df.ADR_ORT
        df["KT_CDU"] = df.ADR_LANDESTEIL
        df["ZIVST_BEZ"] = df.PED_ZIVILSTAND
        df["SPRA_CDI"] = df.PED_SPRACHE        
        df["NATT_CDI"] = df.PED_NATIONALITAT_CODE
        
        df["GEB_DAT"] = "n/a"
        df["ALTER_NP"] = "n/a" 
        aux_index = df[~df.PED_GEBURTSTAG.isnull()].index
        df.loc[aux_index, "GEB_DAT"]  = df.loc[aux_index, "PED_GEBURTSTAG"].apply(lambda x: x.strftime("%d.%m.%Y"))
        df.loc[aux_index, "ALTER_NP"] = df.loc[aux_index, "PED_GEBURTSTAG"].apply(lambda x: self.tranche_timestamp.year - x.year - 
                                            ((self.tranche_timestamp.month, self.tranche_timestamp.day) < (x.month, x.day)))
        
        df["GAGT_BEZ"] = df.KDBTR_MAIN_DE_ASHRF + " " + df.KDBTR_MAIN_DE_NAME
        df["BRTR_NAME_KOMPL"] = df.KDBTR_MAIN_VORNAME + " " + df.KDBTR_MAIN_NACHNAME 
        df["USER_ID_BRTR"] = df.KDBTR_MAIN_PDI_NR
        df["KMPGN-ANTW-ID"] = ""
        df["KMPGN_KNTKT_STAT_BEZ"] = "Selektiert"  
        
        #Anspielung an nonprintsel - Tabelle
        aux_dict = {True: self.tranche_channellist + ["AUSSCHLUSS"], False: self.tranche_channellist}

        if (self.stage == "PROD") & (df.shape[0] > 0): 
            dsel_append_nonprintsel(df[df.KANAL.isin(aux_dict[self.tranche_trackausschluss])], connection_tdb, int(self.campaign_id), self.tranche_number)    
    
        self.tranche_df            = df.reset_index(drop = True)
        self.kpi_size_targetgroup  = df[(df.TRANCHE_KG == 0) & (df.KANAL != "AUSSCHLUSS")].shape[0]
        self.kpi_size_controlgroup = df[(df.TRANCHE_KG == 1) & (df.KANAL != "AUSSCHLUSS")].shape[0]
        self.tranche_status        = "3: Prepared for routing"   
        return
                      
    def route_to_directsales(self, connection_tdb, splitvar = None):
        """Automatically generates csv-files for import into AXA's call center software through Sales Support. 
        The leads are automatically enriched with all necessary customer data for the standard call center 
        conversation script. Several csv-files can be generated by specifying splitvar, along which the 
        csv-files will be split. 
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        connection_tdb << sqlalchemy.engine.base.Engine >> or << sqlalchemy.engine.base.Connection >>
                          Connection as defined by sql_connection()
        splitvar       << str >>
                          Name of an attribute in the tranche's underlying DataFrame along which the 
                          csv-files for disseminating the leads to the telephone channel are split.
                          Used to address different campaign slots of the telephone software. 
                          
        Return Value:
        None
        """ 
        
        #Assertions
        assert(int(self.tranche_status[0]) in (3, 4)), \
        "Data preparation for routing not applied in correct process phase. First assign channels (assign_channel)"
        
        #Preparation
        var_list = ['KMPGN_NAME', 'KMPGN_ID', 'SOURCE_KEY_ID', 'PART_NR', 'PART_PERS_ID', 'TEL_NR_KOMPL_01', 'TEL_NR_KOMPL_02', 'TEL_NR_KOMPL_03', 'TEL_NR_KOMPL_04', \
                    'TEL_NR_KOMPL_05', 'ANREDE_BEZ', 'TITEL_BEZ', 'VNAME', 'NNAME', 'STR_NAME', 'HAUS_NR_KOMPL', 'POFA_NR', 'PLZ', 'ORT_NAME', 'KT_CDU', 'ZIVST_BEZ', \
                    'SPRA_CDI', 'NATT_CDI', 'EMAIL_ADR', 'GEB_DAT', 'ALTER_NP', 'AGT_BEZ', 'GAGT_BEZ', 'BRTR_NAME_KOMPL', 'USER_ID_BRTR', 'KZ_ZEILE_AGGRG', \
                    'KZ_ZEILE_WEIT', 'KMPGN-ANTW-ID', 'POL_NR01', 'POL_NR02', 'POL_NR03', 'POL_NR04', 'POL_NR05', 'IENET_RGSTRG_CD'] + \
                   ['TXT_ATSH_VARBLE_' + str(i).zfill(2) for i in range(1,41)] + ['KMPGN_KNTKT_STAT_BEZ']
               
        aux_df = self.tranche_df[(self.tranche_df.TRANCHE_KG == 0) & (self.tranche_df.KANAL == "TELEFON")].copy()
    
        #Map G: Drive
        for i in range(10):          
            ret = os.system('{}/batchw/CCDA/bat/map_g_drive.bat'.format(sf.platform_is_server("drive")))
            if ret == 0: break
        
        #Generierung CSV
        if self.stage == "PROD": path_export = r"G:\grp\SAPCRM\G04\Telesales\FileTransfer\in"
        else:                    path_export = self.tranche_pathdata
                      
        if (aux_df.shape[0] > 0) & (splitvar == None):
            aux_df[var_list].to_csv(path_export + r"\K{0}_{1}_TR{2}{3}.csv".format(self.campaign_id, 
                                    self.campaign_name, str(self.tranche_number).zfill(4), {"PROD": "", "DEV": "_TELEFON", "ACC": "_TELEFON"}[self.stage]),
                                    index = False, sep = ";", encoding = "latin-1")            
            
        if (aux_df.shape[0] > 0) & (splitvar != None):
            for z in list(aux_df[splitvar].unique()):
                aux_df[aux_df[splitvar] == z][var_list].to_csv(path_export + r"\K{0}_{1}_TR{2}_{3}{4}.csv".format(self.campaign_id, 
                                    self.campaign_name.replace(" ", "_"), str(self.tranche_number).zfill(4), z, {"PROD": "", "DEV": "_TELEFON", "ACC": "_TELEFON"}[self.stage]),
                                    index = False, sep = ";", encoding = "latin-1")  
                                                  
        self.tranche_status = "4: Routed to channels"
        return
        
    def route_to_ad(self, connection_tdb, column_campaign = None, column_de = None, 
                    column_fr = None, column_it = None, id_type_mapping = "PART_NR"):
        """Automatically forwards AD leads to the pilot interface in tdbex for dissemination via SAP Hybris. 
        The leads contain PART_NR and required (technical) information fpr the pilot interface. Additional
        lead information (in 3 languages) can be added to the lead by specifying the corresponding columns.        
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        connection_tdb  << sqlalchemy.engine.base.Engine >> or << sqlalchemy.engine.base.Connection >>
                           Connection as defined by sql_connection()
        column_campaign << str >>
                           Column containing the campaign name to use in pilot interface. If None,
                           the campaign name is set by a default rule. Note that in any case "_ZG" or "_KG"
                           are added automatically to distinguish target & control group
        column_de       << str >>
                           Column containing free text information for the lead in German. 
        column_fr       << str >>
                           Column containing free text information for the lead in French. 
        column_it       << str >>
                           Column containing free text information for the lead in Italian. 
        id_type_mapping << str >> or << dict >>
                           Indicates how to fill the columns "ID" and "ID_TYPE" in the pilot interface.
                           Set string (e.g. "PART_NR") to fill ID_TYPE with this string and fill ID with the values of the column of the same name.
                           Set dict of strings with {"ID_TYPE_COLUMN_NAME": "ID_COLUMN_NAME"} to allow different ID_TYPES in one 
                           transmission to the pilot interface (e.g. with prospects).                            
                                                     
        Return Value:
        None
        """ 
        
        
        #Assertions
        assert(int(self.tranche_status[0]) in (3, 4)), \
        "Data preparation for routing not applied in correct process phase. First assign channels (assign_channel)"
        
        #Preparation
        aux_campaign_name = (self.campaign_id + "_" + self.campaign_name + "_").replace(" ", "_")        
        aux_df = self.tranche_df[self.tranche_df.KANAL == "AD"].copy()
        aux_language_columns = [i for i in [column_de, column_fr, column_it] if i != None] 
        
        #Ensure upper / lower case of aux_language_columns matches columns in data set
        #Required, because standard functions always return full upper or lower case columns
        for i in aux_language_columns: 
            if i.lower() in aux_df.columns: aux_df.rename(columns = {i.lower(): i}, inplace = True)
            if i.upper() in aux_df.columns: aux_df.rename(columns = {i.upper(): i}, inplace = True)
                
        #Generierung Datensatz "marketing_attr_targetgroup"
        if type(id_type_mapping) == str:
            aux_df["ID_TYPE"] = id_type_mapping
            aux_df["ID"]      = aux_df[id_type_mapping].astype(str)
    
        elif type(id_type_mapping) == dict:
            aux_val = list(id_type_mapping.keys())[0]
            aux_df["ID_TYPE"] = aux_df[aux_val]
            aux_df["ID"]      = aux_df[id_type_mapping[aux_val]].astype(str)  
        
        aux_df["ID"]      = aux_df.ID.apply(lambda x: x[:-2] if x[-2:] == ".0" else x)
       
        if column_campaign == None: 
            aux_df["TARGETGROUP"] = aux_df.TRANCHE_KG.map({0: aux_campaign_name + "ZG", 1: aux_campaign_name + "KG"})        
        else: 
            aux_df["TARGETGROUP"] = aux_df[column_campaign] + aux_df.TRANCHE_KG.map({0: "_ZG", 1: "_KG"})
            
        aux_df["TIMESTAMP"] = datetime.datetime.now()     
        aux_df["VALID_UNTIL"] = datetime.date.today() + datetime.timedelta(days = 30)
        aux_df["VALID_UNTIL"] = aux_df.VALID_UNTIL.apply(lambda x: pd._libs.tslib.normalize_date(x))        
        aux_df["RESPONSIBLE_CNR"] = getpass.getuser()

        if self.stage == "PROD":
            sf.sql_todb(aux_df[["ID", "ID_TYPE", "TARGETGROUP", "TIMESTAMP", "VALID_UNTIL", "RESPONSIBLE_CNR"]], \
                        "marketing_targetgroup", connection_tdb, schema = "tdbex", if_exists = "append")
        else:
            aux_df[["ID", "ID_TYPE", "TARGETGROUP", "TIMESTAMP", "VALID_UNTIL", "RESPONSIBLE_CNR"]].to_csv(self.tranche_pathdata + 
                   "\K{0}_{1}_TR{2}{3}.csv".format(self.campaign_id, self.campaign_name, str(self.tranche_number).zfill(4), "_AD_MKTG_TARGETGROUP"), 
                   index = False, sep = ";", encoding = "latin-1")     
                  
        #Generierung Datensatz "marketing_attr_char"
        if len(aux_language_columns) > 0: 
            attrib_df = pd.melt(aux_df, id_vars=['ID'], value_vars = aux_language_columns, var_name='ATTRIB', value_name='VALUE')
            attrib_df = attrib_df.merge(aux_df[["ID", "ID_TYPE", "TIMESTAMP", "VALID_UNTIL", "RESPONSIBLE_CNR"]], how = "left", on = "ID")    
            attrib_df["VALUE"] = attrib_df.VALUE.fillna('').apply(lambda x: x[:4000])

            if self.stage == "PROD":
                sf.sql_todb(attrib_df[["ID", "ID_TYPE", "ATTRIB", "VALUE", "TIMESTAMP", "VALID_UNTIL", "RESPONSIBLE_CNR"]], \
                            "marketing_attr_char", connection_tdb, schema = "tdbex", if_exists = "append")
            else:
                attrib_df[["ID", "ID_TYPE", "ATTRIB", "VALUE", "TIMESTAMP", "VALID_UNTIL", "RESPONSIBLE_CNR"]].to_csv(self.tranche_pathdata + 
                          "\K{0}_{1}_TR{2}{3}.csv".format(self.campaign_id, self.campaign_name, str(self.tranche_number).zfill(4), "_AD_MKTG_ATTRCHAR"), 
                          index = False, sep = ";", encoding = "latin-1")
                      
        self.tranche_status = "4: Routed to channels"
        return
    
    def route_to_hybris(self, interaction_mapping, contact_mapping, nvp_mapping = {}):
        """Automatically forwards leads to SAP Hybris for dissemination into the various channels 
        (AD lead, telephone, email, etc.). 3 mapping dictionaries allow to map columns of self.tranche_df
        to the various parameters of the unterlying SOA API (marketinginteraktiondatenpush_1).
        Cf. the 'soa' module of CCDA_Standard_Functions for more information on this API call.        
            
        Owner: Tobias Ippisch
    
        Keyword Arguments:
        interaction_mapping << dict >>
                               Mapping for the interaction_dict parameter of soa_marketinginteraktiondatenpush_1.
                               Keys include the possible parameters of the SOA service (str), 
                               the values the mapped column names (str) of the pd.DataFrame.
                               Please note that no fix values can be provided with this parameter; 
                               the dict values will always try to reference a pd.DataFrame column. 
                               
        contact_mapping     << dict >>
                               Mapping for the contact_dict parameter of soa_marketinginteraktiondatenpush_1.
                               Keys include the possible parameters of the SOA service (str), 
                               the values the mapped column names (str) of the pd.DataFrame.
                               Please note that no fix values can be provided with this parameter; 
                               the dict values will always try to reference a pd.DataFrame column.
                               
        nvp_mapping         << dict, list (optional) >>
                               Mapping for the nvp_dict parameter of soa_marketinginteraktiondatenpush_1.
                               Keys include the possible parameters of the SOA service (str), 
                               the values the mapped column names (str) of the pd.DataFrame.
                               Please note that no fix values can be provided with this parameter; 
                               the dict values will always try to reference a pd.DataFrame column. 
                               Alternatively, the key-value-pairs can be provided as list of lists or list of tuples.
                                                     
        Return Value:
        None
        """ 
        
        
        #Assertions
        assert(int(self.tranche_status[0]) in (3, 4)), \
        "Data preparation for routing not applied in correct process phase. First assign channels (assign_channel)"
        
        assert(set(self.tranche_df.KANAL.unique()) <= set(["AD", "AUSSCHLUSS", "TELEFON"])), \
        "Attribute 'KANAL' can only have values ['AD', 'AUSSCHLUSS', 'TELEFON']"        
        
        for i in [interaction_mapping, contact_mapping, nvp_mapping]:
            if   type(i) == dict: aux_i = list(i.items())
            elif type(i) == list: aux_i = i
            else: raise TypeError("Mapping dicts not of type dict or list")
                
            for j in aux_i:
                assert(j[1] in self.tranche_df.columns), "Column '{}' as listed in mapping dicts not in self.tranche_df".format(j)                 
        
        #Preparation
        self.tranche_df = self.tranche_df.reset_index(drop = True)
        aux_df = self.tranche_df[self.tranche_df.KANAL != "AUSSCHLUSS"].copy()
        
        #SOA service call 
#        aux_df["API_RESULT"] = aux_df.apply(lambda row: sf.soa_wrapper_marketinginteraktiondatenpush_1(row, interaction_mapping, contact_mapping, nvp_mapping, self.stage), axis = 1)

# 20190516*gep hier Referenz auf importierte Funktion aus Python File Lib soa_gc als so. anstatt sf.
        aux_df["API_RESULT"] = aux_df.apply(lambda row: so.soa_wrapper_marketinginteraktiondatenpush_1(row, interaction_mapping, \
		contact_mapping, nvp_mapping, self.stage), axis = 1)
    
        #Merge SOA service call result to tranche 
        self.tranche_df = self.tranche_df.merge(aux_df[["API_RESULT"]], how = "left", right_index = True, left_index = True)
        self.tranche_status = "4: Routed to SAP Hybris"
        
        #Update Hybris-Report 
        aux_list = list(aux_df.API_RESULT.unique())
        
        if aux_list != ["OK"]:
            aux_str = """<font size="4"><b>yMKT-Schnittstellen-Report</b></font><hr>"""
            for i in aux_list: 
                aux_str += "  - {}x {}<br>".format(aux_df[aux_df.API_RESULT == i].shape[0], i)    
            
            aux_str += "<br><br><br>"
            self.kpi_hybris = aux_str
       
        return 
    
    def report_success(self):
        """Create success report via email to all campaign managers and tech support specified on 
        campaign object initialization. If tranche is run in DEV or ACC, only the technical support 
        staff will be informed. The report includes per default a tranche overview and a de-selection report. 
                
        Owner: Tobias Ippisch 

        Keyword Arguments:
        report_ausschluss << Bool >>
                             Indicates whether leads with channel "AUSSCHLUSS" should be included in the channel report              
                                                     
        Return Value:
        None
        """ 
        
        #Channel Report
        if self.tranche_trackausschluss == True: channel_list = self.tranche_channellist + ["AUSSCHLUSS"]
        else:                                    channel_list = self.tranche_channellist
            
        def channel_reporter(self, channel_list, aux_kg):            
            total_size = self.tranche_df[(self.tranche_df.TRANCHE_KG.isin(aux_kg)) & (self.tranche_df.KANAL.isin(channel_list))].shape[0]
            aux_list = [self.tranche_df[(self.tranche_df.TRANCHE_KG.isin(aux_kg)) & (self.tranche_df.KANAL == i)].shape[0] for i in channel_list]
            aux_list = [[channel_list[i], aux_list[i], round(aux_list[i] * 100 / total_size, 1)] for i in range(len(aux_list)) if aux_list[i] != 0]
            return " / ".join([str(i[0]) + ": " + str(i[1]) + " (" + str(i[2]) + "%)" for i in aux_list])
                    
        channel_report_total = channel_reporter(self, channel_list, [0, 1])
        channel_report_zg    = channel_reporter(self, channel_list, [0])
        
        #Hybris Report 
        if self.kpi_hybris == None: self.kpi_hybris = "" 
        
        #eMail Subject & Body
        email_subject = "{0}Kampagne {1} {2}: Datenaufbereitung erfolgreich".format({"PROD": ""}.get(self.stage, "TESTMODE "), self.campaign_id, self.campaign_name)
        
        email_body = """\
        <html><head></head>
          <body><span style="font-size: 14"><font face="arial">
            <p>Liebe Kampagnen-Verantwortliche,</p>
            <p>die Daten der Kampagne {0} {1} für den {2} stehen zur Weiterverarbeitung bereit.</p><br>
            <font size="4"><b>Tranchen-Report</b></font>
            <hr>
            <b>Identifizierte Kunden total:</b> {3}<br>
            <b>Kunden nach Deselektion:</b> {4} ({5}%)<br>
            <b>Ausgespielte Ziel- / Kontrollgruppe:</b> {6} / {7}<br>
            <b>Kanalzuteilung (Total):</b> {8}<br>
            <b>Kanalzuteilung (Zielgruppe):</b> {9}<br>
            <b>Kampagne:</b> {0}<br>
            <b>Tranche:</b> {10}<br><br><br>
            {11}
            <font size="4"><b>Deselektions-Report</b></font>
            <hr>
            {12}
            <br>
            <br><br>
            Beste Grüsse,<br>
            Competence Center Data & Analytics<br><br>  
    
            <div>
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Python_logo_and_wordmark.svg/230px-Python_logo_and_wordmark.svg.png"  width="115" height="34"><br>
            <span style="font-size: 12"><i>This mail is powered by Python, the Programming Language of Leading Data Scientists.</i></span>     
            </div> 
          </span></font></body>
        </html>""".format(self.campaign_id, self.campaign_name, self.tranche_timestamp.strftime("%d.%m.%Y"),
                          self.kpi_customers_identified, self.kpi_customers_selected, 
                          round(self.kpi_customers_selected / self.kpi_customers_identified * 100, 1),
                          self.kpi_size_targetgroup, self.kpi_size_controlgroup, 
                          channel_report_total, channel_report_zg,
                          self.tranche_number, self.kpi_hybris, "<br>".join(self.kpi_reports))
        
        if self.stage == "PROD": email_to = "; ".join(self.campaign_manager + self.campaign_techsupport)
        else:                    email_to = "; ".join(self.campaign_techsupport)
    
        sf.send_email(subject = email_subject, body = email_body , to = email_to, use_amg = sf.platform_is_server())
        return
                              
    def report_exception(self, traceback):
        """Create exception report via email to all campaign managers and tech support specified at
        campaign object initialization. The report alerts receipients that a tranche run has caused
        and exception. The exception notice is included in the report. 
        If tranche is run in DEV or ACC, only the technical support staff will be informed. 
                
        Owner: Tobias Ippisch                

        Keyword Arguments:
        traceback << str >>
                     Traceback of exception, providing error details
                                                     
        Return Value:
        None
        """ 
        
        
        
        email_subject = "{0}Fehler Datenaufbereitung Kampagne {1} {2}".format({"PROD": ""}.get(self.stage, "TESTMODE "), self.campaign_id, self.campaign_name)
        email_body = """\
        <html><head></head>
          <body><span style="font-size: 14"><font face="arial">
            <p>Liebe Kampagnen-Verantwortliche,</p>
            <p>die Datenaufbereitung der Kampagne {0} {1} für den {2} verursachte einen Fehler.<br>
            Bitte die Weiterverarbeitung besprechen mit: {3}</p><br>
            <p>Fehlerbeschreibung:<br>
            {4}</p><br>            
            <br><br>
            Beste Grüsse,<br>
            Competence Center Data & Analytics<br><br>  
    
            <div>
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Python_logo_and_wordmark.svg/230px-Python_logo_and_wordmark.svg.png"  width="115" height="34"><br>
            <span style="font-size: 12"><i>This mail is powered by Python, the Programming Language of Leading Data Scientists.</i></span>     
            </div> 
          </span></font></body>
        </html>""".format(self.campaign_id, self.campaign_name, self.tranche_timestamp.strftime("%d.%m.%Y"),
                          " / ".join(self.campaign_techsupport), traceback)
        
        if self.stage == "PROD": email_to = "; ".join(self.campaign_manager + self.campaign_techsupport)
        else:                    email_to = "; ".join(self.campaign_techsupport)
    
        sf.send_email(subject = email_subject, body = email_body , to = email_to, use_amg = sf.platform_is_server())        
        return 

    def report_emptydf(self):
        """Send eMail that informs stakeholders of the fact that there is no data (left) in the
        currenct tranceh for the data processing to be continued. An eMail is sent to inform the 
        stakeholders.  
        If tranche is run in DEV or ACC, only the technical support staff will be informed. 
                
        Owner: Tobias Ippisch                
                                                     
        Return Value:
        None
        """ 
        
        email_subject = "{0} Information zur Kampagne {1} {2} - keine neuen Daten verfügbar".format({"PROD": ""}.get(self.stage, "TESTMODE "), self.campaign_id, self.campaign_name)
        email_body = """\
        <html><head></head>
          <body><span style="font-size: 14"><font face="arial">
            <p>Liebe Kampagnen-Verantwortliche,</p>
            <p>für die Kampagne stehen heute keine neuen Daten zur Verfügung. Mögliche Ursachen: <br>
            - Keine neuen Daten von (externer) Schnittstelle geliefert <br>
            - Nach Deselektion / Qualitätsauschlüssen kein Potential mehr vorhanden.</p><br>
            <p>Es gibt aktuell nichts weiter zu tun. Die Kampagne sollte am Folgetag wieder normal laufen.</p><br>            
            <br><br>
            Beste Grüsse,<br>
            Competence Center Data & Analytics<br><br>  
    
            <div>
            <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Python_logo_and_wordmark.svg/230px-Python_logo_and_wordmark.svg.png"  width="115" height="34"><br>
            <span style="font-size: 12"><i>This mail is powered by Python, the Programming Language of Leading Data Scientists.</i></span>     
            </div> 
          </span></font></body>
        </html>""".format(self.campaign_id, self.campaign_name, self.tranche_timestamp.strftime("%d.%m.%Y"),
                          " / ".join(self.campaign_techsupport), traceback)
        
        if self.stage == "PROD": email_to = "; ".join(self.campaign_manager + self.campaign_techsupport)
        else:                    email_to = "; ".join(self.campaign_techsupport)
    
        sf.send_email(subject = email_subject, body = email_body , to = email_to, use_amg = sf.platform_is_server())        
        sys.exit()
        return   

              

#%% 3.1) Personenbezogene Deselektion
def dsel_person_based(df, connection_tdb, report = False, ebene = "PART_NR", min_alter = 0 , max_alter = 100,
                      ausschluss_alterunbekannt  = False, ausschluss_sprache       = False,
                      ausschluss_geschlecht      = False, ausschluss_nationalitaet = False,
                      ausschluss_mitarbeiter     = False, ausschluss_werbesperre   = False,
                      ausschluss_blacklist       = False, ausschluss_keintelefon   = False,
                      ausschluss_keinemail       = False):
    """ Personenbezogene Deselektion
    Deselection of customers (PART_NR) from a DataFrame based on person
    criteria. Employee & "Werbesperre" criteria may be applied either
    on person- or household-level

    The following 4 criteria are applied by default:
    - (aux_df.ADR_KZ_PART_ARCHVG_VGSHN.isnull()) | (aux_df.ADR_KZ_PART_ARCHVG_VGSHN == '0')
    - (aux_df.ADR_ZENTR_ARCHIVIERVMKNG.isnull()) | (aux_df.ADR_ZENTR_ARCHIVIERVMKNG == '0')
    - ADR_IST_VERSTORBEN_JN == 'N'
    - ADR_IST_FIRMA_LIQUIDIERT_JN == 'N'

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                        << pd.DataFrame >>
                                 input DataFrame from which to be de-selected
    report                    << bool >>
                                 indicates whether an additional report-string
                                 is returned
    ebene                     << str (optional)>>
                                 "PART_NR", "HH", or "HAUSHALT"; indicates
                                 whether ausschluss_mitarbeiter &
                                 ausschluss_werbesperre are applied on
                                 person- or household-level)
    min_alter                 << int (optional)>>
                                 minimum age under which customers are
                                 de-selected
    max_alter                 << int (optional)>>
                                 maximum age above which customers are
                                 de-selected
    ausschluss_alterunbekannt << bool (optional)>>
                                 whether to remove customers with unknown age
    ausschluss_sprache        << str (optional)>>
                              << list (optional)>>
                                 2-char country codes of languages to exclude,
                                 False if no de-selection is applied
    ausschluss_nationalitaet  << str (optional)>>
                              << list (optional)>>
                                 2-char country codes of nationalities to exclude,
                                 False if no de-selection is applied
    ausschluss_geschlecht     << str (optional)>>
                              << list (optional)>>
                                 1-char gender specification ("m", "w", "f"
                                 of genders to exclude, False if no
                                 de-selection is applied, 'u' excludes companies
                                 or people with unknown gender)
    ausschluss_mitarbeiter    << bool (optional)>>
                                 whether to remove customers which are
                                 AXA employees
    ausschluss_werbesperre    << bool (optional)>>
                                 whether to remove customers with
                                 "Werbesperre" in place
    ausschluss_blacklist      << bool (optional)>>
                                 whether to remove customers which are on the
                                 TeleSales SI6 blacklist
    ausschluss_keintelefon    << bool (optional)>>
                                 whether to remove customers with
                                 no telephone number
    ausschluss_keinemail      << bool (optional)>>
                                 whether to remove customers with
                                 no email address
    connection_tdb            << sqlalchemy.engine.base.Engine >>
                              << sqlalchemy.engine.base.Connection >>
                                 connection as defined by sql_connection()

    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)

    #Pruefen Verfuegbarkeit PART_NR
    if   "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))

    try:
        #Kopieren Schluesselliste in FBTDBMK
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        aux_df = sf.sql_getdf("""select a.*, b.ped_nationalitat_code
                           from
                               (select part_nr, ped_alter, ped_sprache, ped_geschlecht,
                                adr_ist_axa_ma_jn, adr_ist_axa_ma_jn_hh,
                                adr_werbesperre_jn, adr_werbesperre_jn_hh,
                                adr_ist_verstorben_jn, adr_ist_firma_liquidiert_jn,
                                adr_kz_part_archvg_vgshn, adr_zentr_archiviervmkng,
                                greatest(NVL(adr_email_1_privat, ' '), NVL(adr_email_1_gschftl, ' ')) as adr_email,
                                greatest(NVL(adr_telefon_1_privat, ' '), NVL(adr_telefon_1_gschftl, ' '),
                                         NVL(adr_mobile_1_privat, ' '),  NVL(adr_mobile_1_gschftl, ' ')) as adr_telefon
                                from fbtdbmk.nv_customer_view_dbm_t
                                where stichtag = (select max(distinct stichtag) from fbtdbmk.nv_customer_view_dbm_t)
                                and part_nr in (select part_nr from {0})) a
                          left outer join
                              (select part_nr, ped_nationalitat_code
                               from fbtdbmk.nv_customer_view_base_t
                               where stichtag = (select max(distinct stichtag) from fbtdbmk.nv_customer_view_base_t)) b
                          on a.part_nr = b.part_nr
                          """.format(param_table),
                          connection_tdb, column_lower = False)

        aux_df["PED_GESCHLECHT"] =  aux_df["PED_GESCHLECHT"].fillna('u').apply(lambda x: x[0])

        sf.sql_drop_table(param_table, connection_tdb)
        
        #Ausschluss Min / Max Alter & Alter unbekannt 
        partlist_ausschluss_maxalter = aux_df[aux_df.PED_ALTER > max_alter].PART_NR.unique()
        partlist_ausschluss_minalter = aux_df[aux_df.PED_ALTER < min_alter].PART_NR.unique()
        
        if ausschluss_alterunbekannt: partlist_ausschluss_alterunbekannt = aux_df[aux_df.PED_ALTER.isnull()].PART_NR.unique()
        else:                         partlist_ausschluss_alterunbekannt = []
        
        #Ausschluss Mitarbeiter / Werbesperre
        aux_dict = {"PART_NR": "", "HH": "_HH", "HAUSHALT": "_HH"}
        if ausschluss_mitarbeiter: partlist_ausschluss_mitarbeiter = aux_df[aux_df["ADR_IST_AXA_MA_JN".format(aux_dict[ebene.upper()])] == 'J'].PART_NR.unique() 
        else:                      partlist_ausschluss_mitarbeiter = []
        
        if ausschluss_werbesperre: partlist_ausschluss_werbesperre = aux_df[aux_df["ADR_WERBESPERRE_JN".format(aux_dict[ebene.upper()])] == 'J'].PART_NR.unique() 
        else:                      partlist_ausschluss_werbesperre = []        

        #Ausschluss Sprache
        if type(ausschluss_sprache) == str: ausschluss_sprache = [ausschluss_sprache]
        if ausschluss_sprache != False: partlist_ausschluss_sprache = aux_df[aux_df.PED_SPRACHE.isin(ausschluss_sprache)].PART_NR.unique() 
        else:                           partlist_ausschluss_sprache = []
                                                                          
        #Ausschluss Nationalität
        if type(ausschluss_nationalitaet) == str: ausschluss_nationalitaet = [ausschluss_nationalitaet]
        if ausschluss_nationalitaet != False: partlist_ausschluss_nationalitaet = aux_df[aux_df.PED_NATIONALITAT_CODE.isin(ausschluss_nationalitaet)].PART_NR.unique() 
        else:                                 partlist_ausschluss_nationalitaet = []        
        
        #Ausschluss Geschlecht
        if type(ausschluss_geschlecht) == str: ausschluss_geschlecht = [ausschluss_geschlecht]
        elif type(ausschluss_geschlecht) == list: ausschluss_geschlecht = [i[0].lower().replace('f', 'w') for i in ausschluss_geschlecht]
        
        if ausschluss_geschlecht != False: partlist_ausschluss_geschlecht = aux_df[aux_df.PED_GESCHLECHT.isin(ausschluss_geschlecht)].PART_NR.unique() 
        else:                              partlist_ausschluss_geschlecht = []
        
        #Ausschluss kein Telefon / eMail
        if ausschluss_keintelefon: partlist_ausschluss_keintelefon = aux_df[aux_df.ADR_TELEFON.isin(['', ' '])].PART_NR.unique() 
        else:                      partlist_ausschluss_keintelefon = []
        
        if ausschluss_keinemail: partlist_ausschluss_keinemail = aux_df[aux_df.ADR_EMAIL.isin(['', ' '])].PART_NR.unique() 
        else:                    partlist_ausschluss_keinemail = []   

        #Ausschluss Blacklist ALT bis Sep 2017
#        if ausschluss_blacklist:
#            try:
#                aux_blacklist = pd.read_excel(r"\\ch.doleni.net\DFS\GRP\TS_TELESALES\Organisationen\Sales-Support\SI6\Blacklist\Blacklist_SI6.xlsx", \
#                                              sheet = "Tabelle1").Partner_Nummer.fillna(0).astype(int).unique()
#            except IOError:
#                print("SI6 Blacklist konnte nicht aus Excel-File geladen werden; Blacklist auf 'leer' gesetzt.")
#                aux_blacklist = []
#
#            partlist_ausschluss_blacklist = aux_df[aux_df.PART_NR.isin(aux_blacklist)].PART_NR.unique()
#        else: partlist_ausschluss_blacklist = []

        #Ausschluss Blacklist NEU ab Sep 2017 via HANA Blacklist, in CRM gepflegt
        if ausschluss_blacklist:  
            try:
                con_hana_prod= sf.sql_connection("hana_prod",user = 'H905226', pw = sf.get_password("hana_prod", 'H905226'))
                sql_statement = """
                SELECT DISTINCT PART_NR_INT4 AS PART_NR
                FROM "_SYS_BIC"."axach.prod.global.bol.partner/CA_PART_KNTKT"
                WHERE ZZKONTDVSPERRE = 'X' OR ZBLACKLIST_DS = 'Ja'
                """
                aux_blacklist = sf.sql_getdf(sql_statement, con_hana_prod, column_lower = False).PART_NR.unique()
            except:
                print("SI6 Blacklist konnte nicht aus HANA CRM View geladen werden; Blacklist auf 'leer' gesetzt.")
                aux_blacklist = []
                
            partlist_ausschluss_blacklist = aux_df[aux_df.PART_NR.isin(aux_blacklist)].PART_NR.unique()
        else: partlist_ausschluss_blacklist = []
               
        #Ausschluss gesetzte Bedingungen (inaktive Partner, geloesche Records, verstorbene Personen, liquidierte Firmen)
        partlist_ausschluss_verstorben = aux_df[aux_df.ADR_IST_VERSTORBEN_JN == 'J'].PART_NR.unique() 
        partlist_ausschluss_liquidiertefirma = aux_df[aux_df.ADR_IST_FIRMA_LIQUIDIERT_JN == 'J'].PART_NR.unique() 
        partlist_ausschluss_archivierungvorgesehen = aux_df[(aux_df.ADR_KZ_PART_ARCHVG_VGSHN.notnull()) & (aux_df.ADR_KZ_PART_ARCHVG_VGSHN != '0')].PART_NR.unique()
        partlist_ausschluss_archivierungsvormerkung = aux_df[(aux_df.ADR_ZENTR_ARCHIVIERVMKNG.notnull()) & (aux_df.ADR_ZENTR_ARCHIVIERVMKNG != '0')].PART_NR.unique()
                                                            
        #Generierung Report
        if report:
            reportfile = u"<b>Personenbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: {0}<br>".format(df.shape[0])
            reportfile += u"  - Deselektierte Kunden über max. Alter: {0}<br>".format(len(partlist_ausschluss_maxalter))
            reportfile += u"  - Deselektierte Kunden unter min. Alter: {0}<br>".format(len(partlist_ausschluss_minalter))

            if ausschluss_alterunbekannt:
                reportfile += u"  - Deselektierte Kunden Alter unbekannt: {0}<br>".format(len(partlist_ausschluss_alterunbekannt))

            if ausschluss_sprache != 0:
                reportfile += u"  - Deselektierte Kunden aufgrund Sprache: {0}<br>".format(len(partlist_ausschluss_sprache))

            if ausschluss_nationalitaet != 0:
                reportfile += u"  - Deselektierte Kunden aufgrund Nationalität: {0}<br>".format(len(partlist_ausschluss_nationalitaet))

            if ausschluss_geschlecht != 0:
                reportfile += u"  - Deselektierte Kunden aufgrund Geschlecht: {0}<br>".format(len(partlist_ausschluss_geschlecht))

            if ausschluss_mitarbeiter:
                reportfile += u"  - Deselektierte Kunden Mitarbeiter: {0}<br>".format(len(partlist_ausschluss_mitarbeiter))

            if ausschluss_werbesperre:
                reportfile += u"  - Deselektierte Kunden Werbesperre: {0}<br>".format(len(partlist_ausschluss_werbesperre))

            if ausschluss_keintelefon:
                reportfile += u"  - Deselektierte Kunden ohne Telefonnummer: {0}<br>".format(len(partlist_ausschluss_keintelefon))

            if ausschluss_keinemail:
                reportfile += u"  - Deselektierte Kunden ohne eMail-Adresse: {0}<br>".format(len(partlist_ausschluss_keinemail))
                
            if ausschluss_blacklist:
                reportfile += u"  - Deselektierte Kunden SI6 Blacklist: {0}<br>".format(len(partlist_ausschluss_blacklist))

            reportfile += u"  - Deselektierte Kunden verstorben: {0}<br>".format(len(partlist_ausschluss_verstorben))
            reportfile += u"  - Deselektierte Kunden liquidierte Firma: {0}<br>".format(len(partlist_ausschluss_liquidiertefirma))
            reportfile += u"  - Deselektierte Kunden vorgesehene Archivierung: {0}<br>".format(len(partlist_ausschluss_archivierungvorgesehen))
            reportfile += u"  - Deselektierte Kunden Archivierungsvormerkung: {0}<br>".format(len(partlist_ausschluss_archivierungsvormerkung))
        
        else: reportfile = ""
            
        #Anwendung Deselektionskriterien
        df = df[(~df[aux_part_nr].isin(partlist_ausschluss_maxalter)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_minalter)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_alterunbekannt)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_sprache)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_nationalitaet)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_geschlecht)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_mitarbeiter)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_werbesperre)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_keintelefon)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_keinemail)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_blacklist)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_verstorben)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_liquidiertefirma)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_archivierungsvormerkung)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_archivierungvorgesehen))]

        if report:
            reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]    
            
        return aux_dsel_returner(df, aux_tranche, report, reportfile)     
         
    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.2) Vertragsbezogene Deselektion
def dsel_contract_based(df, connection_tdb, report = False, ausschluss_pc = False, ausschluss_ls = False, \
                        ausschluss_kl = False, ausschluss_hypo = False, ausschluss_custom_pc = False, branchen_custom_pc = None):
    """ Vertragsbezogene Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on contract information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                   << pd.DataFrame >>
                            input DataFrame from which to be de-selected
    report               << bool >>
                            indicates whether an additional report-string is
                            returned
    ausschluss_pc        << int (optional)>>
                            indicates whether customers with P&C contracts
                            are de-selected;
                            
                            set -1 if customers with only such contracts
                                       are to be de-selected  
                            set a positive integer x if customers with a least 
                                       x such contracts are to be de-selected  
                                       
    ausschluss_ls        << int (optional)>>
                            indicates whether customers with L&S contracts
                            are de-selected;
                            
                            set -1 if customers with only such contracts
                                       are to be de-selected  
                            set a positive integer x if customers with a least 
                                       x such contracts are to be de-selected  
                                       
    ausschluss_kl        << int (optional)>>
                            indicates whether customers with KL contracts are
                            de-selected;
                            
                            set -1 if customers with only such contracts
                                       are to be de-selected  
                            set a positive integer x if customers with a least 
                                       x such contracts are to be de-selected 
                                       
    ausschluss_hypo      << int (optional)>>
                            indicates whether customers with Hypo / mortgage
                            contracts are de-selected;
                            
                            set -1 if customers with only such contracts
                                       are to be de-selected  
                            set a positive integer x if customers with a least 
                                       x such contracts are to be de-selected     
                                       
    ausschluss_custom_pc << int (optional)>>
                            indicates whether customers with custom-defined P&C
                            products (e.g. MF) are de-selected; if string
                            is provided, a list of products in
                            branchen_custom_pc must be provided;
                            
                            set -1 if customers with only such contracts
                                       are to be de-selected  
                            set a positive integer x if customers with a least 
                                       x such contracts are to be de-selected
                                       
    branchen_custom_pc   << list (optional) >>
                            custom P&C products on which customers are de-selected
                            if they have them; list may contain product codes
                            (e.g. 99003, 93000) or
                            "MF","HR","HA","INT","RSV","UVG"
    connection_tdb       << sqlalchemy.engine.base.Engine >>
                         << sqlalchemy.engine.base.Connection >>
                            connection as defined by sql_connection()

    Return Value:
    pd.DataFrame (subset of input DataFrame, reduced by de-selection criteria)
    (optional) string (de-selection reporting string)
    """    
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    
    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))

    try:
        #Kopieren Schluesselliste in FBTDBMK
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        aux_df = sf.sql_getdf("""select part_nr, ges, nrb, pol_nr, peb_prod_angebot
                               from tdbmk.agr_aktpol_w_v
                               where cor_stichtag_yyyyww = (select max(cor_stichtag_yyyyww) from tdbmk.agr_aktpol_w_v)
                               and part_nr in (select part_nr from """ + param_table + """)""",
                           connection_tdb, column_lower = False)

        sf.sql_drop_table(param_table, connection_tdb)

        #Hilfs-Listen: Kunden mit P&C / L&S / KL / Hypotheken - Policen (Spaltenname, GES, Listen mit NRBs)
        aux_list = [["POL_PC",   "U", ["01"]                                                                        ], \
                    ["POL_LS",   "L", ["01", "02", "03", "04", "05", "06", "07", "08", "09", "32", "33", "34", "35"]], \
                    ["POL_KL",   "L", ["10", "11", "12", "13", "14", "15", "16", "17"]                              ], \
                    ["POL_HYPO", "W", ["20"]                                                                        ]]

        aggregate_df = pd.DataFrame(aux_df.PART_NR.unique(), columns = ["PART_NR"]) 
        
        #Generierung aggregierter Policen-Informationen für 4 Hauptbranchen
        for i in aux_list:
            aux_df2 =  pd.DataFrame(aux_df[(aux_df.GES == i[1]) & (aux_df.NRB.isin(i[2]))].groupby("PART_NR").POL_NR.count())
            aux_df2.columns = [i[0]]

            aggregate_df = aggregate_df.merge(aux_df2, how = "left", left_on = "PART_NR", right_index = True)
        
        #Generierung aggregierter Policen-Informationen für Custom P&C Branchen
        if ausschluss_custom_pc == False: aux_branchen = ["0"]
        else: aux_branchen = [str(i) for i in sf.format_mapping(branchen_custom_pc, "produkte")]
        
        aux_df2 =  pd.DataFrame(aux_df[(aux_df.GES == aux_list[0][1]) & (aux_df.NRB.isin(aux_list[0][2])) & \
                                       (aux_df.PEB_PROD_ANGEBOT.isin(aux_branchen))].groupby("PART_NR").POL_NR.count())
        aux_df2.columns = ["POL_CUSTOMPC"]      
                                 
        aggregate_df = aggregate_df.merge(aux_df2, how = "left", left_on = "PART_NR", right_index = True)
        
        #Datenbereinigung
        aggregate_df.fillna(0, inplace = True) 

        #Generierung Part-Listen fuer Ausschluesse nach P&C, L&S, KL und Hypotheken
        def aux_select_contract_by_branche(aggregate_df, condition, product):
            product_list = ["POL_PC", "POL_LS", "POL_KL", "POL_HYPO"]
            
            #only
            if condition == -1:  
                aux_list = [i for i in product_list if i != product]                
                partlist = list(aggregate_df[(aggregate_df[aux_list].sum(axis = 1) == 0) & (aggregate_df[product] > 0)].PART_NR.unique())   
            
            #more than x            
            elif condition > 0:  partlist = list(aggregate_df[aggregate_df[product] >= condition].PART_NR.unique())            
            else: partlist = []
            return partlist

        partlist_ausschluss_pc   =      aux_select_contract_by_branche(aggregate_df, ausschluss_pc,        "POL_PC")
        partlist_ausschluss_ls   =      aux_select_contract_by_branche(aggregate_df, ausschluss_ls,        "POL_LS")
        partlist_ausschluss_kl   =      aux_select_contract_by_branche(aggregate_df, ausschluss_kl,        "POL_KL")
        partlist_ausschluss_hypo =      aux_select_contract_by_branche(aggregate_df, ausschluss_hypo,      "POL_HYPO")
        partlist_ausschluss_custom_pc = aux_select_contract_by_branche(aggregate_df, ausschluss_custom_pc, "POL_CUSTOMPC")

        #Generierung Report
        if report:
            reportfile = u"<b>Vertragsbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: %s<br>" %df.shape[0]

            if ausschluss_pc == -1:
                reportfile += u"  - Deselektierte Kunden nur mit P&C Verträgen: {0}<br>".format(len(partlist_ausschluss_pc))
            if ausschluss_pc > 0:
                reportfile += u"  - Deselektierte Kunden mit mindestens {0} P&C Verträgen: {1}<br>".format(ausschluss_pc, len(partlist_ausschluss_pc))

            if ausschluss_ls == -1:
                reportfile += u"  - Deselektierte Kunden nur mit L&S Verträgen: {0}<br>".format(len(partlist_ausschluss_ls))
            if ausschluss_ls > 0:
                reportfile += u"  - Deselektierte Kunden mit mindestens {0} L&S Verträgen: {1}<br>".format(ausschluss_ls, len(partlist_ausschluss_ls))
                
            if ausschluss_kl == -1:
                reportfile += u"  - Deselektierte Kunden nur mit KL Verträgen: {0}<br>".format(len(partlist_ausschluss_kl))
            if ausschluss_kl > 0:
                reportfile += u"  - Deselektierte Kunden mit mindestens {0} KL Verträgen: {1}<br>".format(ausschluss_kl, len(partlist_ausschluss_kl))

            if ausschluss_hypo == -1:
                reportfile += u"  - Deselektierte Kunden nur mit Hypotheken: {0}<br>".format(len(partlist_ausschluss_hypo))
            if ausschluss_hypo > 0:
                reportfile += u"  - Deselektierte Kunden mit mindestens {0} Hypotheken: {1}<br>".format(ausschluss_hypo, len(partlist_ausschluss_hypo))

            if ausschluss_custom_pc == -1:
                reportfile += u"  - Deselektierte Kunden nur mit spezifischen P&C Verträgen: {0}<br>".format(len(partlist_ausschluss_custom_pc))
            if ausschluss_custom_pc > 0:
                reportfile += u"  - Deselektierte Kunden mit mindestens {0} spezifischen P&C Verträgen: {1}<br>".format(ausschluss_custom_pc, len(partlist_ausschluss_custom_pc))

        else: reportfile = ""
        
        #Anwendung Deselektionskriterien
        df = df[(~df[aux_part_nr].isin(partlist_ausschluss_pc)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_ls)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_kl)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_hypo)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_custom_pc))]

        if report:
            reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]
                        
        return aux_dsel_returner(df, aux_tranche, report, reportfile)    



    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.3) Ortsbezogene Deselektion
def dsel_location_based(df, connection_tdb, report = False, ausschluss_ausland = False, \
                        ausschluss_liechtenstein = False, ausschluss_campione_buesingen = False):
    """ Ortsbezogene Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on location information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                            << pd.DataFrame >>
                                     input DataFrame from which to be
                                     de-selected
    report                        << bool >>
                                     indicates whether an additional
                                     report-string is returned
    ausschluss_ausland            << bool (optional)>>
                                     indicates whether customers living outside
                                     of CH / FL are de-selected
    ausschluss_liechtenstein      << bool (optional)>>
                                     indicates whether customers living in FL
                                     are de-selected
    ausschluss_campione_buesingen << bool (optional)>>
                                     indicates whether customers in
                                     Campione / Buesingen are de-selected
    connection_tdb                << sqlalchemy.engine.base.Engine >>
                                  << sqlalchemy.engine.base.Connection >>
                                     connection as defined by sql_connection()

    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    
    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))

    try:
    #Kopieren Schluesselliste in FBTDBMK
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        aux_df = sf.sql_getdf("""select part_nr, adr_land, adr_ist_liechtenstein_jn, adr_ist_campione_buesingen_jn
                          from fbtdbmk.nv_customer_view_dbm_t
                          where stichtag = (select max(distinct stichtag) from fbtdbmk.nv_customer_view_dbm_t)
                          and part_nr in (select part_nr from """ + param_table + """)""",
                          connection_tdb, column_lower=False)

        sf.sql_drop_table(param_table, connection_tdb)
        
        #Ausschluss Ausland
        if ausschluss_ausland: partlist_ausschluss_ausland = aux_df[~aux_df.ADR_LAND.isin(['CH', 'FL'])].PART_NR.unique()
        else:                  partlist_ausschluss_ausland = []

        #Ausschluss Liechtenstein
        if ausschluss_liechtenstein: partlist_ausschluss_liechtenstein = aux_df[aux_df.ADR_IST_LIECHTENSTEIN_JN == 'J'].PART_NR.unique()
        else:                        partlist_ausschluss_liechtenstein = []

        #Ausschluss Campione / Büsingen
        if ausschluss_ausland: partlist_ausschluss_campionebuesingen = aux_df[aux_df.ADR_IST_CAMPIONE_BUESINGEN_JN == 'J'].PART_NR.unique()
        else:                  partlist_ausschluss_campionebuesingen = []        
        

        #Generierung Report
        if report:
            reportfile = u"<b>Ortsbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: {0}<br>".format(df.shape[0])
            if ausschluss_ausland:
                reportfile += u"  - Deselektierte Kunden Ausland: {0}<br>".format(len(partlist_ausschluss_ausland))
            if ausschluss_liechtenstein:
                reportfile += u"  - Deselektierte Kunden Liechtenstein: {0}<br>".format(len(partlist_ausschluss_liechtenstein))
            if ausschluss_campione_buesingen:
                reportfile += u"  - Deselektierte Kunden Campione / Büsingen: {0}<br>".format(len(partlist_ausschluss_campionebuesingen))

        else: reportfile = ""
        
        #Anwendung Deselektionskriterien
        df = df[(~df[aux_part_nr].isin(partlist_ausschluss_ausland)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_liechtenstein)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_campionebuesingen))]

        if report:
            reportfile += u"Einträge nach Deselektion: {0}<br>".format(df.shape[0])
                        
        return aux_dsel_returner(df, aux_tranche, report, reportfile)    
        
    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.4) Vertriebsbezogene Deselektion
def dsel_distribution_based(df, connection_tdb, report = False, ausschluss_broker           = False,    ausschluss_ad             = False,
                                                                ausschluss_direkt           = False,    ausschluss_post           = False, 
                                                                ausschluss_novartissyngenta = False,    ausschluss_postfinancezkb = False, 
                                                                ausschluss_embassy          = False,    ausschluss_spezialbestand = False, 
                                                                ausschluss_rahmenvertrag    = False,    ausschluss_downsellgefahr = False):
    """ Vertriebsbezogene Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on distribution information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                          << pd.DataFrame >>
                                   input DataFrame from which to be de-selected
    report                      << bool >>
                                   indicates whether an additional
                                   report-string is returned
    ausschluss_broker           << bool (optional)>>
                                   set False to not apply this criterion
                                   set True to de-select all customers having
                                   at least one such policy
                                << list (optional)>>
                                   indicates whether customers with
                                   broker-contracts are de-selected;
                                   set list of product codes on which to
                                   de-select customers if they have at least
                                   one such policy in the corresponding
                                   distribution channel; list may contain
                                   product codes (e.g. 99003, 93000) or
                                   "MF", "HR", "HA", "INT", "RSV", "UVG"
    ausschluss_ad               << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with
                                   ad-contracts are de-selected;
                                   cf. ausschluss_broker for details
    ausschluss_direkt           << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with
                                   direct-contracts are de-selected;
    						           cf. ausschluss_broker for details
    ausschluss_post             << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with
                                   post-contracts are de-selected;
    						           cf. ausschluss_broker for details
    ausschluss_novartissyngenta << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with
                                   Novartis/Syngenta-contracts are de-selected;
                                   cf. ausschluss_broker for details
    ausschluss_postfinancezkb   << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with
                                   PostFinance/ZKB-contracts are de-selected;
                                   Especially useful for L&S campaigns
                                   cf. ausschluss_broker for details                                   
    ausschluss_embassy          << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with 
                                   embassy-contracts are de-selected;
                                   cf. ausschluss_broker for details
    ausschluss_spezialbestand   << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with 
                                   contracts in special portfolios are de-selected;
                                   those include Nord/Domizil and Broker Competenza
                                   Kilchberg
                                   cf. ausschluss_broker for details
    ausschluss_rahmenvertrag    << bool (optional)>>
                                << list (optional)>>
                                   indicates whether customers with at least one
                                   Rahmenvertrag-contract are de-selected
                                   Set False if no customers are de-selected 
                                   Set True to consider all Rahmenvertrag-contraacts
                                   for de-selection
                                   Set list of form [71, 141] to de-select customers
                                   which have contracts covered by specific Rahmenvertrag-partners
                                   List of Rahmenvertrag-partners: 
                                   http://one.axa.com/wps/wcm/myconnect/17dec60047dbd387b374bb6730bcf60b/
                                   Rahmenvertr%C3%A4ge+in+V3_de.pdf?MOD=AJPERES&CACHEID=17dec60047dbd387b374bb6730bcf60b                                   
    ausschluss_downsellgefahr   << bool (optional)>>                              
                                   indicates whether customers with at
                                   least 1 motor contract with a car older
                                   than 6 years and comprehensive coverage 
                                   (Vollkasko) are de-selected                                   
    connection_tdb              << sqlalchemy.engine.base.Engine >>
                                << sqlalchemy.engine.base.Connection >>
                                   connection as defined by sql_connection()

    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    
    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df

    #Mapping von Produkten auf Produkt-Codes
    ausschluss_broker           = sf.format_mapping(ausschluss_broker,           "produkte")
    ausschluss_ad               = sf.format_mapping(ausschluss_ad,               "produkte")
    ausschluss_direkt           = sf.format_mapping(ausschluss_direkt,           "produkte")
    ausschluss_post             = sf.format_mapping(ausschluss_post,             "produkte")
    ausschluss_novartissyngenta = sf.format_mapping(ausschluss_novartissyngenta, "produkte")
    ausschluss_postfinancezkb   = sf.format_mapping(ausschluss_postfinancezkb,   "produkte")
    ausschluss_embassy          = sf.format_mapping(ausschluss_embassy,          "produkte")   
    ausschluss_spezialbestand   = sf.format_mapping(ausschluss_spezialbestand,   "produkte")

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))

    try:
        #Kopieren Schluesselliste in FBTDBMK
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        sql_statement = """select a.*, b.de_typ_cddev, c.pol_nr_rahmenvert
                          from
                              (select part_nr, pol_nr, peb_prod_angebot, org_nlel_kanal_b, org_ls_ga_b, org_nl_vp_adma_b
                               from tdbmk.agr_aktpol_w_v
                               where cor_stichtag_yyyyww = (select max(cor_stichtag_yyyyww) from tdbmk.agr_aktpol_w_v)
                               and part_nr in (select part_nr from {0})) a

                          left outer join
                              (select vpagt, max(de_typ_cddev) as de_typ_cddev
                               from bwdm.part_dev_vpcode_v
                               where gueltig_bis = to_date('31.12.9999', 'dd.mm.YYYY')
                               group by vpagt) b
                          on a.org_nl_vp_adma_b = b.vpagt

                          left outer join
                              (select pol_nr, max(pol_nr_rahmenvert) as pol_nr_rahmenvert, max(rahmenvert) as rahmenvert
                              from bwdm.agrnl_agreement_v
                              where ersi_yyyy = 9999
                              and ges = 'U'
                              and nrb = '01'
                              group by pol_nr) c
                          on a.pol_nr = c.pol_nr""".format(param_table)
                
        aux_df = sf.sql_getdf(sql_statement, connection_tdb, column_lower=False)

        aux_df["DE_TYP_CDDEV"]      = aux_df.DE_TYP_CDDEV.fillna(0).astype(int)
        aux_df["POL_NR_RAHMENVERT"] = aux_df.POL_NR_RAHMENVERT.fillna(0).astype(int)

        #Zusatzkriterium Ausschluss zusaetzlicher Broker-Bestaende & Anpassung ORG_NLEL_KANAL_B fuer Ausschluss ueber Standard-Kriterien)
        aux_df.loc[aux_df[aux_df.DE_TYP_CDDEV.isin([15, 20, 57, 58, 59, 61, 66])].index, "ORG_NLEL_KANAL_B"] = "BROKER"

        #Zuatzkriterium Ausschluss Novartis / Syngenta Mitarbeiter & Anpassung ORG_NL_VP_ADMA_B fuer Ausschluss ueber Standard-Kriterien)
        aux_df.loc[aux_df[aux_df.POL_NR_RAHMENVERT.isin([52, 15])].index, "ORG_NL_VP_ADMA_B"] = 80390

        #Allgemeine Ausschluss-Funktion
        def aux_ausschuss_function(q, attribute, criterion):
            if   q == False: return []
            elif q == True:  return aux_df[aux_df[attribute].isin(criterion)].PART_NR.unique()
            else:
                q = [str(i) for i in q]
                return aux_df[(aux_df[attribute].isin(criterion)) & (aux_df.PEB_PROD_ANGEBOT.isin(q))].PART_NR.unique()

        partlist_ausschluss_broker           = aux_ausschuss_function(ausschluss_broker,           "ORG_NLEL_KANAL_B", ["BROKER"])
        partlist_ausschluss_ad               = aux_ausschuss_function(ausschluss_ad,               "ORG_NLEL_KANAL_B", ["AD"])
        partlist_ausschluss_direkt           = aux_ausschuss_function(ausschluss_direkt,           "ORG_NLEL_KANAL_B", ["DIREKT"])
        partlist_ausschluss_post             = aux_ausschuss_function(ausschluss_post,             "ORG_NL_VP_ADMA_B", [10265])
        partlist_ausschluss_novartissyngenta = aux_ausschuss_function(ausschluss_novartissyngenta, "ORG_NL_VP_ADMA_B", [80390, 80391, 80392, 80393, 80388])
        partlist_ausschluss_embassy          = aux_ausschuss_function(ausschluss_embassy,          "ORG_NL_VP_ADMA_B", [140931, 140937, 145937]) #145937 nur in Huls Liste
        partlist_ausschluss_spezialbestand   = aux_ausschuss_function(ausschluss_spezialbestand,   "ORG_NL_VP_ADMA_B", [940598, 140118])        
        partlist_ausschluss_postfinancezkb   = aux_ausschuss_function(ausschluss_postfinancezkb,   "ORG_LS_GA_B",      ["J6", "43"])  
        #Spezialbestände beinhalten: Nord/Domizil, Broker Competenza Kilchberg
        #L&S GA Listen: http://one.axa.com/wps/wcm/myconnect/0101718047dc11359e799f6730bcf60b/Adressliste_GA_GAVV.xlsx?MOD=AJPERES&attachment=true&CACHE=NONE&CONTENTCACHE=NONE
        
        #Bedingung Ausschluss Rahmenverträge: 
        if   ausschluss_rahmenvertrag  == False:     partlist_ausschluss_rahmenvertrag = []
        elif ausschluss_rahmenvertrag  == True:      partlist_ausschluss_rahmenvertrag = aux_df[(aux_df.POL_NR_RAHMENVERT != 0) | (aux_df.RAHMENVERT != 0)].PART_NR.unique() 
        elif type(ausschluss_rahmenvertrag) == list: partlist_ausschluss_rahmenvertrag = aux_df[aux_df.POL_NR_RAHMENVERT.isin(ausschluss_rahmenvertrag)].PART_NR.unique() 
        
        #Bedingung Ausschluss Downsell-Gefahr Policen Älter als 6 Jahre mit Vollkasko:
        if ausschluss_downsellgefahr == True: 
            sql_statement = """select a.*
                               from
                                   (select part_nr, pol_nr
                                    from tdbmk.agr_aktpol_w_v
                                    where cor_stichtag_yyyyww = (select max(cor_stichtag_yyyyww) from tdbmk.agr_aktpol_w_v)
                                    and peb_prod_angebot in ('99000', '99003', '99006')
                                    and part_nr in (select part_nr from {0})) a
    
                               inner join
                                   (select distinct pol_nr
                                    from bwdm.agrnl_agreement_v
                                    where ersi_yyyy = 9999
                                    and ges = 'U'
                                    and nrb = '01'
                                    and v_gfhr_cdu in (99021, 99023, 99026, 99028)
                                    and net_praemie_sfr > 0
                                    group by pol_nr) b
                               on a.pol_nr = b.pol_nr
                              
                               inner join
                                   (select c.pol_nr 
                                        from
                                        (select pol_nr, eing_dat, min(invse_dat) as invse_dat
                                             from bwdm.agrnl_mfv_fahr_v
                                             where ersi_yyyy = 9999
                                             and ges = 'U'
                                             and nrb = '01'
                                             group by pol_nr, eing_dat) c 
                                        
                                        left outer join 
                                
                                        (select pol_nr, eing_dat, min(invse_dat) as invse_dat
                                             from bwdm.agrnl_mfv_fahr_v
                                             where ersi_yyyy = 9999
                                             and ges = 'U'
                                             and nrb = '01'
                                             group by pol_nr, eing_dat) d 
                                        
                                        on c.pol_nr = d.pol_nr
                                        and c.eing_dat < d.eing_dat        
                                        where d.eing_dat IS NULL
                                        and c.invse_dat < to_date('{1}', 'dd.mm.YYYY')) e

                               on a.pol_nr = e.pol_nr""".format(param_table, (datetime.datetime.today() - 
                                                                datetime.timedelta(days = 6 * 365)).strftime("%d.%m.%Y"))       
            
            partlist_ausschluss_downsellgefahr = sf.sql_getdf(sql_statement, connection_tdb, column_lower=False)
            partlist_ausschluss_downsellgefahr = partlist_ausschluss_downsellgefahr.PART_NR.unique()
        
        else: partlist_ausschluss_downsellgefahr = [] 
        
        #Entfernen TEMP-Tabelle auf TDB
        sf.sql_drop_table(param_table, connection_tdb)    
        
        #Generierung Report
        if report:
            reportfile = u"<b>Vertriebsbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: %s<br>" %df.shape[0]

            if ausschluss_broker != 0:
                reportfile += u"  - Deselektierte Kunden Broker-Kanal: %s<br>" %len(partlist_ausschluss_broker)

            if ausschluss_ad != 0:
                reportfile += u"  - Deselektierte Kunden AD-Kanal: %s<br>" %len(partlist_ausschluss_ad)

            if ausschluss_direkt != 0:
                reportfile += u"  - Deselektierte Kunden Direkt-Kanal: %s<br>" %len(partlist_ausschluss_direkt)

            if ausschluss_post != 0:
                reportfile += u"  - Deselektierte Kunden Post: %s<br>" %len(partlist_ausschluss_post)

            if ausschluss_novartissyngenta != 0:
                reportfile += u"  - Deselektierte Kunden Novartis / Syngenta: %s<br>" %len(partlist_ausschluss_novartissyngenta)

            if ausschluss_postfinancezkb != 0:
                reportfile += u"  - Deselektierte Kunden PostFinance / ZKB: %s<br>" %len(partlist_ausschluss_postfinancezkb)
                
            if ausschluss_embassy != 0:
                reportfile += u"  - Deselektierte Kunden Embassy-Bestand: %s<br>" %len(partlist_ausschluss_embassy)
                
            if ausschluss_spezialbestand != 0:
                reportfile += u"  - Deselektierte Kunden Spezial-Bestand: %s<br>" %len(partlist_ausschluss_spezialbestand)

            if ausschluss_rahmenvertrag != 0:
                reportfile += u"  - Deselektierte Kunden mit mind. 1 Rahmenvertrag-Police: %s<br>" %len(partlist_ausschluss_rahmenvertrag)                   

            if ausschluss_downsellgefahr != 0:
                reportfile += u"  - Deselektierte Kunden mit Downsell-Gefahr (MF, 6 Jahre mit Vollkasko): %s<br>" %len(partlist_ausschluss_downsellgefahr)    

        else: reportfile = ""
        
        #Anwendung Deselektionskriterien
        df = df[(~df[aux_part_nr].isin(partlist_ausschluss_broker)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_ad)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_direkt)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_post)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_novartissyngenta)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_postfinancezkb)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_embassy)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_spezialbestand)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_rahmenvertrag)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_downsellgefahr))]

        if report:
            reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]

        return aux_dsel_returner(df, aux_tranche, report, reportfile)



    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.5) Eventbasierte Deselektion
def dsel_event_based(df, connection_tdb, report = False, fuer_produkt = False, connection_px33 = None,
                     ausschluss_neugeschaeft         = False, ausschluss_mutation      = False,
                     ausschluss_storno               = False, ausschluss_sistierung    = False,
                     ausschluss_wiederinkraftsetzung = False, ausschluss_offerte       = False,
                     ausschluss_ablauf               = False, ausschluss_ablauf_future = False,
                     ausschluss_mahnung              = False, ausschluss_betreibung    = False,
                     ausschluss_vna                  = False):
    """ Eventbasierte Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on distribution information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                              << pd.DataFrame >>
                                       input DataFrame from which to be
                                       de-selected
    report                          << bool >>
                                       indicates whether an additional
                                       report-string is returned
    fuer_produkt                    << bool (optional)>>
                                       set True or False if de-selection
                                       criteria are applied to all products
                                    << list (optional)>>
                                       indicated whether de-selection criteria
                                       are applied to particular products only;
    						           set list of product codes if de-selection
                                       criteria are only applied for specific
                                       products; list may contain product codes
                                       (e.g. 99003, 93000) or
                                       "MF", "HR", "HA", "INT", "RSV", "UVG"
    ausschluss_neugeschaeft         << int (optional)>>
                                       number of days in which from today a new
                                       business must have occured for the
                                       customer to be de-selected
    ausschluss_mutation             << int (optional)>>
                                       number of days in which from today a
                                       mutation must have occured for the
                                       customer to be de-selected
    ausschluss_storno               << int (optional)>>
                                       number of days in which from today a
                                       cancellation must have occured for the
                                       customer to be de-selected
    ausschluss_sistierung           << int (optional)>>
                                       number of days in which from today a
                                       suspension must have occured for the
                                       customer to be de-selected
    ausschluss_wiederinkraftsetzung << int (optional)>>
                                       number of days in which from today a
                                       re-installation must have occured for
                                       the customer to be de-selected
    ausschluss_offerte              << int (optional)>>
                                       number of days in which from today a
                                       V3 quote must have been issued for
                                       the customer to be de-selected
    ausschluss_ablauf               << int (optional)>>
                                       number of days in which from today a
                                       P&C policy must have experienced for
                                       the customer to be de-selected
    ausschluss_ablauf_future        << int (optional)>>
                                       number of days in which from today a
                                       P&C policy must expire in the future for
                                       the customer to be de-selected                                        
    ausschluss_mahnung              << int (optional)>>
                                       number of days in which from today a
                                       FINEX Mahngebühr must have been charged for
                                       the customer to be de-selected
    ausschluss_betreibung           << int (optional)>>
                                       number of days in which from today a
                                       FINEX Betreibungsgebühr must have been charged for
                                       the customer to be de-selected
    ausschluss_vna                  << int (optional)>>
                                       number of days in which from today a
                                       VNA (Versicherungsnachweis) must have been issed for
                                       the customer to be de-selected
    ausschluss_beschwerde           << not implemented >>
    ausschluss_bvm                  << not implemented >>
    ausschluss_schaden              << not implemented >>
    ausschluss_schaden_offen        << not implemented >>
    connection_tdb                  << sqlalchemy.engine.base.Engine >>
                                    << sqlalchemy.engine.base.Connection >>
                                       connection as defined by sql_connection()
    connection_px33                 << sqlalchemy.engine.base.Engine >>
                                    << sqlalchemy.engine.base.Connection >>
                                       connection to PX33 as defined by sql_connection()

    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    
    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df

    #Mapping von Produkten auf Produkt-Codes
    fuer_produkt = sf.format_mapping(fuer_produkt, "produkte")
    if fuer_produkt in [True, False]: ausschluss_fuer_produkt = "aux_df.V_PROD_CDU != 0"
    else:
        fuer_produkt = [str(i) for i in fuer_produkt]
        ausschluss_fuer_produkt = "aux_df.V_PROD_CDU.isin([" + ", ".join(fuer_produkt) + "])"

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))
    param_max_timeback = max(ausschluss_neugeschaeft, ausschluss_mutation, ausschluss_storno, ausschluss_sistierung, 
                             ausschluss_wiederinkraftsetzung, ausschluss_ablauf)
    param_max_timeback = datetime.date.today() - datetime.timedelta(days = param_max_timeback)
    param_timeback_quotes = datetime.date.today() - datetime.timedelta(days = ausschluss_offerte)

    try:
        #Ausschluss Kriterien basierend auf Aktpol (Neugeschaeft, Mutation, Storno, Sistierung, Wiederinkraftsetzung)
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        aux_df = sf.sql_getdf("""select a.part_nr, b.v_prod_cdu, b.gv_typ_cdva, b.eing_dat, b.vv_abl_dat, b.ersi_yyyy
                          from
                              (select distinct part_nr, ges, nrb, pol_nr
                               from tdbmk.agr_aktpol_v
                               where cor_stichtag_yyyymm >= {0}
                               and ges = 'U'
                               and nrb = '01'
                               and part_nr in (select part_nr from {1})) a
                          left outer join
                              (select distinct ges, nrb, pol_nr, v_prod_cdu, gv_typ_cdva, eing_dat, vv_abl_dat, ersi_yyyy
                               from bwdm.agrnl_agreement_v
                               where ersi_dat >= to_date('{2}', 'dd.mm.YYYY')
                               and ersi_yyyy >= {3}) b
                          on a.ges = b.ges and a.nrb = b.nrb and a.pol_nr = b.pol_nr""".
                          format((param_max_timeback - datetime.timedelta(days = 32)).strftime('%Y%m'),
                                 param_table,
                                 param_max_timeback.strftime('%d.%m.%Y'),
                                 str(param_max_timeback.year-1)),
                          connection_tdb, column_lower=False)

        aux_df["VV_ABL_DAT"] = aux_df.VV_ABL_DAT.apply(lambda x: sf.to_datetime(x, "high")) 

        partlist_ausschluss_neugeschaeft = []
        today = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0, 0))
        if ausschluss_neugeschaeft != False:
            aux_df.EING_DAT.dtype
            partlist_ausschluss_neugeschaeft = aux_df[(aux_df.EING_DAT > today - datetime.timedelta(days = ausschluss_neugeschaeft)) & \
                                                      (aux_df.GV_TYP_CDVA.isin([1,2])) & \
                                                      (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_mutation = []
        if ausschluss_mutation != False:
            partlist_ausschluss_mutation = aux_df[(aux_df.EING_DAT > today - datetime.timedelta(days = ausschluss_mutation)) & \
                                                  (aux_df.GV_TYP_CDVA.isin([3,4])) & \
                                                  (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_storno = []
        if ausschluss_storno != False:
            partlist_ausschluss_storno = aux_df[(aux_df.EING_DAT > today - datetime.timedelta(days = ausschluss_storno)) & \
                                                (aux_df.GV_TYP_CDVA.isin([8,9,10])) & \
                                                (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_sistierung = []
        if ausschluss_sistierung != False:
            partlist_ausschluss_sistierung = aux_df[(aux_df.EING_DAT > today - datetime.timedelta(days = ausschluss_sistierung)) & \
                                                    (aux_df.GV_TYP_CDVA.isin([5])) & \
                                                    (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_wiederinkraftsetzung = []
        if ausschluss_wiederinkraftsetzung != False:
            partlist_ausschluss_wiederinkraftsetzung = aux_df[(aux_df.EING_DAT > today - datetime.timedelta(days = ausschluss_wiederinkraftsetzung)) & \
                                                              (aux_df.GV_TYP_CDVA.isin([6])) & \
                                                              (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_ablauf = []
        if ausschluss_ablauf != False:
            partlist_ausschluss_ablauf = aux_df[(aux_df.ERSI_YYYY == 9999) & \
                                                (aux_df.VV_ABL_DAT <= today) & \
                                                (aux_df.VV_ABL_DAT >= today - datetime.timedelta(days = ausschluss_ablauf)) & \
                                                (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        partlist_ausschluss_ablauf_future = []
        if ausschluss_ablauf_future != False:
            partlist_ausschluss_ablauf_future = aux_df[(aux_df.ERSI_YYYY == 9999) & \
                                                (aux_df.VV_ABL_DAT > today) & \
                                                (aux_df.VV_ABL_DAT <= today + datetime.timedelta(days = ausschluss_ablauf_future)) & \
                                                (eval(ausschluss_fuer_produkt))].PART_NR.unique()

        #Ausschluss Kriterien basierend auf Offertdaten (Offertbestellung)
        partlist_ausschluss_offerte = []
        if ausschluss_offerte != False :
            aux_df = sf.sql_getdf("""select distinct part_nr, v_prod_cdu
                              from bwdm.agrnl_offerte_v
                              where erstg_zpkt_dat >= to_date('""" + param_timeback_quotes.strftime('%d.%m.%Y') + """', 'dd.mm.YYYY')
                              and part_nr in (select part_nr from """ + param_table + """)""",
                              connection_tdb, column_lower=False)

            partlist_ausschluss_offerte = aux_df[(eval(ausschluss_fuer_produkt))].PART_NR.unique()
        
        #Ausschluss Kriterien basierend auf Finex-Daten (Mahnung & Betreibung)
        partlist_ausschluss_mahnung = []
        partlist_ausschluss_betreibung = []

        if ausschluss_betreibung + ausschluss_mahnung > 0:
            aux_dat_mahnung = datetime.datetime.now() - datetime.timedelta(days = ausschluss_mahnung)
            aux_dat_betreibung = datetime.datetime.now() - datetime.timedelta(days = ausschluss_betreibung)

            aux_df = sf.sql_leftjoin(df, connection = connection_tdb,
                                  schema = "fbtdbmk", tablename = "nv_customer_view_directsales_t",
                                  key = "PART_NR",
                                  attributes = ["DATUM_MAHNUNG", "DATUM_BETREIBUNG"],
                                  column_lower = False)

            if ausschluss_mahnung != False:
                partlist_ausschluss_mahnung = aux_df[aux_df.DATUM_MAHNUNG > aux_dat_mahnung].PART_NR.unique()

            if ausschluss_betreibung != False:
                partlist_ausschluss_betreibung = aux_df[aux_df.DATUM_BETREIBUNG > aux_dat_betreibung].PART_NR.unique()


        #Deselektion von Kunden mit vorgängiger VNA-Bestellung (via PX33)
        #GV-Codewerte: VNA erstellen via GUI (1), V3 (41), VNAS (42) und Prozess Emil Frey (28)
        partlist_ausschluss_vna = []
        if ausschluss_vna != False:
            if not connection_px33: raise TypeError("Ausschluss VNA requested, but no connection to PX33 defined.")

            sql_statement = """select distinct policennummer as pol_nr
                               from px33.utstat_gv
                               where gv_art in (1, 41, 42, 28)
                               and datum >= to_date('{0}', 'dd.mm.YYYY')""".format((datetime.datetime.now() - datetime.timedelta(days = ausschluss_vna)).strftime("%d.%m.%Y"))

            aux_df = sf.sql_getdf(sql_statement, connection = connection_px33, column_lower = False)

            aux_df = sf.sql_leftjoin(aux_df, connection = connection_tdb,
                      schema = "tdbmk", tablename = "agr_aktpol_w_v",
                      key = "POL_NR",
                      attributes = "PART_NR",
                      conditions = ["cor_stichtag_id = (select max(distinct cor_stichtag_id) from tdbmk.agr_aktpol_w_v)",
                                    "ges = 'U'",
                                    "nrb = '01'"],
                      column_lower = False)

            partlist_ausschluss_vna = df[df.PART_NR.isin(aux_df.PART_NR.unique())].PART_NR.unique()

        #Entfernen TEMP-Tabelle
        sf.sql_drop_table(param_table, connection_tdb)

        #Generierung Report
        if report:
            reportfile = u"<b>Eventbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: %s<br>" %df.shape[0]

            if ausschluss_neugeschaeft != False:
                reportfile += u"  - Deselektierte Kunden Neugeschäft: %s<br>" %len(partlist_ausschluss_neugeschaeft)

            if ausschluss_mutation != False:
                reportfile += u"  - Deselektierte Kunden Mutation: %s<br>" %len(partlist_ausschluss_mutation)

            if ausschluss_storno != False:
                reportfile += u"  - Deselektierte Kunden Storno: %s<br>" %len(partlist_ausschluss_storno)

            if ausschluss_sistierung != False:
                reportfile += u"  - Deselektierte Kunden Sistierung: %s<br>" %len(partlist_ausschluss_sistierung)

            if ausschluss_wiederinkraftsetzung != False:
                reportfile += u"  - Deselektierte Kunden Wiederinkraftsetzung: %s<br>" %len(partlist_ausschluss_wiederinkraftsetzung)

            if ausschluss_offerte != False:
                reportfile += u"  - Deselektierte Kunden mit Offline-Offerten: %s<br>" %len(partlist_ausschluss_offerte)

            if ausschluss_ablauf != False:
                reportfile += u"  - Deselektierte Kunden mit kürzlichem Ablauf: %s<br>" %len(partlist_ausschluss_ablauf)
                
            if ausschluss_ablauf_future != False:
                reportfile += u"  - Deselektierte Kunden mit zeitnahem Ablauf in Zukunft: %s<br>" %len(partlist_ausschluss_ablauf_future)                

            if ausschluss_mahnung != False:
                reportfile += u"  - Deselektierte Kunden mit kostenpflichtiger Mahnung: %s<br>" %len(partlist_ausschluss_mahnung)

            if ausschluss_betreibung!= False:
                reportfile += u"  - Deselektierte Kunden mit Betreibung: %s<br>" %len(partlist_ausschluss_betreibung)

            if ausschluss_vna!= False:
                reportfile += u"  - Deselektierte Kunden mit VNA-Bestellung: %s<br>" %len(partlist_ausschluss_vna)

        else: reportfile = ""
        
        #Anwendung Deselektionskriterien (auf df und nicht aux_df da fehlende POL_NR nicht zum Ausschluss fuehren sollten (hat ggf. Leben-Police)
        df = df[(~df[aux_part_nr].isin(partlist_ausschluss_neugeschaeft)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_mutation)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_storno)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_sistierung)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_wiederinkraftsetzung)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_offerte)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_ablauf)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_ablauf_future)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_mahnung)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_betreibung)) & \
                (~df[aux_part_nr].isin(partlist_ausschluss_vna))]

        if report:
            reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]

        return aux_dsel_returner(df, aux_tranche, report, reportfile)



    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.6) Kontaktbezogene Deselektion
def dsel_contact_based(df, connection_tdb, report = False, ausschluss_kontakt = 0,     ausschluss_kontakt_future = 30,
                                                           ausschluss_kampagne = 0,    ausschluss_kampagne_dict = None,
                                                           ausschluss_nonprintsel = 0, ausschluss_nonprintsel_dict = None):
    """ Kontaktbezogene Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on contact information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                          << pd.DataFrame >>
                                   input DataFrame from which to be de-selected
    report                      << bool >>
                                   indicates whether an additional report-string
                                   is returned
    ausschluss_kontakt          << int (optional)>>
                                   number of days in which from today a contact must
                                   have occured for the customer to be de-selected
    ausschluss_kontakt_future   << int (optional)>>
                                   number of days from today in the future in which 
                                   a contact must occur for the customer to be de-selected                              
    ausschluss_kampagne         << int (optional)>>
                                   number of days in which from today a campaign
                                   contact must have occured for the customer
                                   to be de-selected (uses bwdm.crm_aktivitaet and
                                   bwdm.crm_kontaktdoku_ad)
    ausschluss_kampagne_dict    << dict (optional)>>
                                   Dictionary of campaign id and an indicator whether
                                   customers in a specific campagne are de-selected, e.g.
                                   {'91316': True, '95143': False}
                                   Note that customers in campaigns not specified in the dict
                                   will be de-selected by default to avoid contact collisions                                  
                                   Campaigns not in the list will be listed in the report file
    ausschluss_nonprintsel      << int (optional)>>
                                   number of days in which from today a campaign
                                   contact must have occured for the customer
                                   to be de-selected (uses tdbmk.non_print_sel)
    ausschluss_nonprintsel_dict << dict (optional)>>
                                   Dictionary of campaign id and an indicator whether
                                   customers in a specific campagne are de-selected, e.g.
                                   {'91316': True, '95143': False}
                                   Note that customers in campaigns not specified in the dict
                                   will be de-selected by default to avoid contact collisions
                                   Campaigns not in the list will be listed in the report file                                                                 
    connection_tdb              << sqlalchemy.engine.base.Engine >>
                                << sqlalchemy.engine.base.Connection >>
                                   connection as defined by sql_connection()

    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    
    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return df
        pass

    #Bestimmung Ausschlusszeitpunkt
    date_ausschluss_kontakt     = (datetime.date.today() - datetime.timedelta(days = ausschluss_kontakt)).strftime('%d.%m.%Y')
    date_ausschluss_kampagne    = (datetime.date.today() - datetime.timedelta(days = ausschluss_kampagne)).strftime('%d.%m.%Y')
    date_ausschluss_nonprintsel = (datetime.date.today() - datetime.timedelta(days = ausschluss_nonprintsel)).strftime('%d.%m.%Y')
    date_ausschluss_future      = (datetime.date.today() + datetime.timedelta(days = ausschluss_kontakt_future)).strftime('%d.%m.%Y')

    #SQL-Parameter: Bestimmung Hilfstabelle FBTDBMK
    param_table = 'temp' + str(int(random() * 100000))
    
    #Hilfsfunktion zum Standardisieren der Kampagnennummern (mit und ohne "K" am Anfang)
    def standardize_dict(aux_dict):
        k_numbers     = [[i, aux_dict[i]] for i in aux_dict.keys() if     re.match("[0-9]", i)] 
        other_numbers = [[i, aux_dict[i]] for i in aux_dict.keys() if not re.match("[0-9]", i)] 
        aux_numbers      = [["K"+i[0], i[1]] for i in k_numbers]
        return {i[0]: i[1] for i in (k_numbers + other_numbers + aux_numbers)}

    try:
        #Kopieren Schluesselliste in FBTDBMK
        sf.sql_todb(df[aux_part_nr].drop_duplicates(), param_table, connection_tdb)

        #Abfrage fuer Ansprache via Kontakt / Kontakt-Dokumentation
        #In jedem Fall nur persönliche Kontakte (Face-to-Face, Telefon) für Ausschluss berücksichtigt
        #Bei gepflegten Kontakten zudem nur bestimmte Kontaktkategorien ausgeschlossen,
        #in der (nachgängigen Kontakt-Dokumentation alle)
        #Einträge in der Kontakt-Dokumentation haben zudem keinen Status (BLG_STAT_KETTE)
        #BLG_STAT_KETTE: Z001E0001 = Offen, Z001E0002 = Geplant, Z001E0014 = Erledigt
        #KATEG: ZBR = Beratungstermin NL, ZER = Ersttermin, ZGB = Gesamtberatung, ZFO = Folgetermin,
        #KOMKN_KANAL_GRP_CDMKTG: 100 = Telefon, 400 = Messenger (Telefon!), 500 = Face-to-Face

        sql_statement = """select distinct part_nr
                                from bwdm.crm_act_aktivitaet_v
                                where TERMIN_AKTL_VON > to_date('{0}', 'dd.mm.YYYY')
                                and   TERMIN_AKTL_VON < to_date('{1}', 'dd.mm.YYYY')
                                and BLG_STAT_KETTE in ('Z001E0001', 'Z001E0002', 'Z001E0014')
                                and KATEG in ('ZBR', 'ZER', 'ZGB', 'ZFO')
                                and KOMKN_KANAL_GRP_CDMKTG in (100, 400, 500)
                                and part_nr in (select part_nr from {2})

                           union

                           select distinct part_nr
                                from bwdm.crm_act_kontaktdoku_ad_v
                                where ERSTG_ZPKT > to_date('{0}', 'dd.mm.YYYY')
                                and   ERSTG_ZPKT < to_date('{1}', 'dd.mm.YYYY')
                                and KOMKN_KANAL_GRP_CDMKTG in (100, 400, 500)
                                and part_nr in (select part_nr from {2})""".format(date_ausschluss_kontakt, date_ausschluss_future, param_table)

        aux_df_kon = sf.sql_getdf(sql_statement, connection_tdb, column_lower=False)
        aux_df_kon = aux_df_kon.drop_duplicates()

        #Abfrage fuer Ansprache via Kampagne
        #kmpgn_id not like 'I%' schliesst reine Info-Kampagnen aus (beginnen mit 'I')
        aux_df_kamp = sf.sql_getdf("""select part_nr, kmpgn_id
                          from bwdm.crm_kmpgn_part_v
                          where eingesetzt > to_date('{0}', 'dd.mm.YYYY')
                          and kmpgn_id not like 'I%'
                          and ((KMPGN_ZIEL_GRP_ART_CDCRM = 'ZE0001' and KMPGN_PART_STAT_CDCRM in ('E0001', 'E0002', 'E0004', 'E0007', 'E0008', 'E0010')) or
                               (KMPGN_ZIEL_GRP_ART_CDCRM = 'ZE0002' and KMPGN_PART_STAT_CDCRM in ('E0002')))
                          and part_nr in (select part_nr from {1}) """.format(date_ausschluss_kampagne, param_table),
                          connection_tdb, column_lower=False)

        aux_df_kamp["KMPGN_ID"] = aux_df_kamp.KMPGN_ID.apply(lambda x: x.strip())
        aux_df_kamp["AUX_AUSSCHLUSS"] = True

        if ausschluss_kampagne_dict != None:
            ausschluss_kampagne_dict = standardize_dict(ausschluss_kampagne_dict)
            aux_df_kamp["AUX_AUSSCHLUSS"] = aux_df_kamp.KMPGN_ID.apply(lambda x: ausschluss_kampagne_dict.get(x, True))

        #Abfrage fuer Ansprache via unadressierte Kampagnen via non_print_sel
        aux_df_nonprintsel = sf.sql_getdf("""select part_nr, knum
                                          from tdbmk.non_print_sel
                                          where update_date > to_date('{0}', 'dd.mm.YYYY')
                                          and knum not like 'I%'
                                          and part_nr in (select part_nr from {1}) """.format(date_ausschluss_nonprintsel, param_table),
                                          connection_tdb, column_lower=False)

        aux_df_nonprintsel["KNUM"] = aux_df_nonprintsel.KNUM.apply(lambda x: x.strip())
        aux_df_nonprintsel["AUX_AUSSCHLUSS"] = True

        if ausschluss_nonprintsel_dict != None:
            ausschluss_nonprintsel_dict = standardize_dict(ausschluss_nonprintsel_dict)
            aux_df_nonprintsel["AUX_AUSSCHLUSS"] = aux_df_nonprintsel.KNUM.apply(lambda x: ausschluss_nonprintsel_dict.get(x, True))

        sf.sql_drop_table(param_table, connection_tdb)
        
        #Generierung Report
        if report:
            reportfile = u"<b>Kontaktbezogene Deselektion</b><br>"
            reportfile += u"Einträge vor Deselektion: {}<br>".format(df.shape[0])
            if ausschluss_kontakt != 0:
                reportfile += u"  - Deselektierte Kunden mit vorgängigem Kontakt: {}<br>".format(aux_df_kon.shape[0])
            if ausschluss_kampagne != 0:
                reportfile += u"  - Deselektierte Kunden andere Kampagnen: {}<br>".format(aux_df_kamp[aux_df_kamp.AUX_AUSSCHLUSS == True].PART_NR.nunique())
            if ausschluss_nonprintsel != 0:
                reportfile += u"  - Deselektierte Kunden unadressierte Kampagnen (nonprintsel): {}<br>".format(aux_df_nonprintsel[aux_df_nonprintsel.AUX_AUSSCHLUSS == True].PART_NR.nunique())

        #Anwendung Deselektionskriterien
        if ausschluss_kontakt  != 0: df = df[(~df[aux_part_nr].isin(aux_df_kon.PART_NR))]
        if ausschluss_kampagne != 0: df = df[(~df[aux_part_nr].isin(aux_df_kamp[aux_df_kamp.AUX_AUSSCHLUSS == True].PART_NR.unique()))]
        if ausschluss_nonprintsel != 0: df = df[(~df[aux_part_nr].isin(aux_df_nonprintsel[aux_df_nonprintsel.AUX_AUSSCHLUSS == True].PART_NR.unique()))]
        
        if report:
            reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]
            
            #Reporting zusätzlich nicht berücksichtigte Kampagnen (ausschluss_kampagne_dict)
            aux_list = []
            if ausschluss_kampagne_dict != None:
                aux_list = [i for i in aux_df_kamp.KMPGN_ID.unique() if i not in list(ausschluss_kampagne_dict.keys())]
            if (ausschluss_kampagne != 0) & (len(aux_list) > 0):
                aux_text = u"<b><font color='red'>ACHTUNG:</font></b> Zusätzliche neue Kampagnen (ausschluss_kampagne_dict) ausgeschlossen, bitte prüfen: {}<br>".format(', '.join(aux_list))
                reportfile += aux_text
                print(aux_text[:-4])
            
            #Reporting zusätzlich nicht berücksichtigte Kampagnen (ausschluss_nonprintsel_dict)
            aux_list = []
            if ausschluss_nonprintsel_dict != None:
                aux_list = [i for i in aux_df_nonprintsel.KNUM.unique() if i not in list(ausschluss_nonprintsel_dict.keys())]
            if (ausschluss_nonprintsel != 0) & (len(aux_list) > 0):
                aux_text = u"<b><font color='red'>ACHTUNG:</font></b> Zusätzliche neue Kampagnen (ausschluss_nonprintsel_dict) ausgeschlossen, bitte prüfen: {}<br>".format(', '.join(aux_list))
                reportfile += aux_text
                print(aux_text[:-4])                
        
        else: reportfile = ""
        
        return aux_dsel_returner(df, aux_tranche, report, reportfile)



    except Exception:
        print(traceback.format_exc())
        sf.sql_drop_table(param_table, connection_tdb)
        return df

#%% 3.6) Backup-bezogene Deselektion
def dsel_backup_based(df, campaign, report = False, stage = "DEV", ausschluss_daysback = True):
    """ Eventbasierte Deselektion
    Deselection of customers (PART_NR) from a DataFrame
    based on distribution information.

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                  << pd.DataFrame >>
                           input DataFrame from which to be de-selected
    campaign            << campaign object >>
                           Campaign object holding backup with customers in the campaign in the past
    report              << bool >>
                           indicates whether an additional report-string is returned
    stage               << str >>
                           Indicates stage on which the current deselection is run. 
                           It may be necessary to set stage = "DEV" so not all customers are 
                           de-selected here and subsequent campaign (reporting) steps produce meaningful results
    ausschluss_daysback << int (optional) >>
                           Days from today in which the customer might have been contacted on order 
                           to deselect the customer. Set True to deselect all customers in the backup
                           (regardless of when they have been deselected)
    
    Return Value:
    << pd.DataFrame >>
       subset of input DataFrame, reduced by de-selection criteria
    << str (optional) >>
       de-selection reporting string
    """
    
    #Prüfen Input auf Type df oder tranche
    df, aux_tranche, report =  aux_dsel_starter(df, report)
    aux_val_vorher = df.shape[0]

    df_backup = campaign.campaign_backup 
    
    if stage == "PROD":
        if type(ausschluss_daysback) == int: 
            aux_list = df_backup[df_backup.TRANCHE_DATUM > (datetime.datetime.now() - datetime.timedelta(days = ausschluss_daysback))].PART_NR.unique()
            df = df[(~df.PART_NR.isin(aux_list))]        
        else: 
            df = df[~df.PART_NR.isin(df_backup.PART_NR)]
                   
        df.reset_index(drop = True, inplace = True)    
    
    if report:      
        reportfile = u"<b>Deselektion bezogen auf Kampagne {0} {1}</b><br>".format(campaign.campaign_id, campaign.campaign_name) 
        reportfile += u"Einträge vor Deselektion: {0}<br>".format(aux_val_vorher)
        reportfile += u"  - Deselektierte Kunden, die bereits angegangen wurden: {0}<br>".format(aux_val_vorher - df.shape[0])
        reportfile += u"Einträge nach Deselektion: %s<br>" %df.shape[0]   
        
    else: 
        reportfile = ""
        
    return aux_dsel_returner(df, aux_tranche, report, reportfile)   



#%% 3.7) Befüllung Tabelle Nicht-Prospero-Kampagnenselektionen (non_print_sel)
def dsel_append_nonprintsel(df, connection_tdb, campaign_id, tranche = 0):
    """ Befüllung Tabelle Nicht-Prospero-Kampagnenselektionen
    Writes information on customers selected for campaigns not uploaded to Prospero to the shared table
    tdbmk.non_print_sel. This provides an otherwise non-available data source for deselecting approached customers.
    Please notice that only base attributes are provided.
    These include NP_ID (ID that cannot be empty), PART_NR, KNUM (campaign_id), and UPDATE_DATE (time the data is appended).
    As NP_ID campaign_id + upload month + upload date will be used (note that including year in addition would lead to
    integrity errors on the field NP_ID, i.e. the values would be too large).

    Owner: Tobias Ippisch

    Keyword Arguments:
    df                        << pd.DataFrame >>
                                 input DataFrame (with PART_NR)
    connection_tdb            << sqlalchemy.engine.base.Engine >>
                              << sqlalchemy.engine.base.Connection >>
                                 connection as defined by sql_connection()
    campaign_id               << int >>
                                 AXA campaign ID
    tranche                   << int (optional)>>
                                 Campaign tranche

    Return Value:
    None
    """

    #Pruefen Verfuegbarkeit PART_NR
    if "PART_NR" in df.columns.values: aux_part_nr = "PART_NR"
    elif "part_nr" in df.columns.values: aux_part_nr = "part_nr"
    else:
        print("PART_NR / part_nr nicht im angespielten Datensatz. Bitte hinzufügen.")
        return

    param_now = datetime.datetime.now()

    nonprint_df = df[[aux_part_nr]].reset_index(drop = True).copy()
    nonprint_df["NP_ID"] = nonprint_df.index
    nonprint_df["NP_ID"] = nonprint_df["NP_ID"].apply(lambda x: int("{}{}{}".format(campaign_id, param_now.strftime("%m%d"), x)))
    nonprint_df["KNUM"] = str(campaign_id)
    nonprint_df["TNUM"] = tranche
    nonprint_df["UPDATE_DATE"] = param_now

    sf.sql_todb(nonprint_df, tablename = "non_print_sel", connection = connection_tdb, schema = "tdbmk", if_exists = "append")
    return

def aux_dsel_starter(df, report):
    """For internal use in combination with dsel_functions"""
    if type(df) == sf.campaign_management.Tranche:
        return df.tranche_df, df, True
    elif type(df) in [pd.core.frame.DataFrame, pd.core.series.Series]:
        return df, None, report
    else:
        raise AssertionError("Check of Input-DataFrame caused error. Invalid type.")

def aux_dsel_returner(df, aux_tranche, report, reportfile):
    """For internal use in combination with dsel_functions"""            
    if   (aux_tranche == None) & (report == True):  return df, reportfile       
    elif (aux_tranche == None) & (report == False): return df
    else:
        #Assertions
        assert(1 <= int(aux_tranche.tranche_status[0]) <= 2), \
        "Customer de-selection not applied in correct process phase. Perform on new tranche"
        
        aux_tranche.tranche_df = df
        aux_tranche.tranche_status = "2: Deselected" 
        aux_tranche.kpi_customers_selected = df.shape[0]
        aux_tranche.kpi_reports += [reportfile]   
        return aux_tranche
