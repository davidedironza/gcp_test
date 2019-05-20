# -*- coding: utf-8 -*-
try: 
    #0 Modul-Import & Parametrierung    
    #0.1 Modul-Import

    from google.cloud import bigquery
    client = bigquery.Client(location="europe-west6")
    from google.cloud import storage

    # change these to try this notebook out
    BUCKET = 'axa-ch-raw-dev-dla'
    PROJECT = 'axa-ch-datalake-analytics-dev'
    REGION = 'eu-west6'

    import re
    import random
    import locale
    import traceback
    import pandas as pd
    import pickle
    import datetime
    import pytz
    import time
    import os
    import importlib
    import tempfile
    import sys
    sys.path.insert(0, '../functions')

    import auxiliary_gc as aux
    importlib.reload(aux)

    import campaign_management_gc as cm
    importlib.reload(cm)

    #import functions.soa_gc as soa
    import soa_gc as soa
    importlib.reload(soa)

    #0.2 Pfadinformationen
    path_data_va = "bindexis/data/various/"
    path_data_input = "bindexis/data/input/"

    path_data = 'gs://axa-ch-raw-dev-dla/bindexis/data/various/kampagne.pkl'    

    #0.3 Set timezone
    os.environ['TZ'] = 'Europe/Zurich'
    time.tzset()

    # Retrieve Bindexis Data
    #0.4 Access & Authentication Setup
    print('0.4')
    time_now      = datetime.datetime.now(pytz.timezone('Europe/Zurich'))

    client_cs = storage.Client()
    bucket_cs = client_cs.get_bucket(BUCKET)

    print("except: pickle file kann nicht geöffnet werden, daher campaign_timelastrun als Tagesdatum - 1")
    campaign_timelastrun = time_now + datetime.timedelta(days=-1)

    print(campaign_timelastrun)


    # Achtung dieses Zertifikat muss von Sandor noch neu erstellt werden
    # und wir müssen die Standardfunktion soa_marketinginteraktiondatenpush_1
    # hierauf noch anpassen im Bereich #A) Parameter Setting & Function Definition 

    # function for setting expiration date of a table - mainly fpr temporary tables
    def tmp_table_expiration(table_ref_in, minutes_in):
        table = client.get_table(table_ref_in) 
    #assert table.expires is None
        expiration = datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=minutes_in)
        table.expires = expiration
        table_ref = client.update_table(table, ["expires"])  # API request

    # function needed for check if created table already present or the program must set a short sleep 
    # until creation is finished
    def bq_tbl_exists(client, table_ref):
        from google.cloud.exceptions import NotFound
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False 

    # needed for checking if bq temporary table exists
    project_nm = 'axa-ch-datalake-analytics-dev'
    dataset_nm = 'temp_da'
    table_nm = 'tmp_bau_projects'

    client = bigquery.Client(project=project_nm,location="europe-west6")

    dataset = client.dataset(dataset_nm)
    table_ref = dataset.table(table_nm)    

    # org stage = sf.platform_is_server("stage")
    stage = 'ACC'
    locale.getlocale()
    # locale.setlocale(locale.LC_ALL, "German") 

    # get plz for main agencies - the original csv. file is stored in a bucket
    # plz_df = pd.read_csv(r'gs://axa-ch-raw-dev-dla/bindexis/data/various/plz_dict.csv', sep=';', header =[0] )

    sql_statement = """SELECT * FROM `axa-ch-datalake-analytics-dev.various_da.va_ga_plz`"""
    plz_df = client.query(sql_statement).to_dataframe().drop_duplicates()
    #plz_df.head()

    ## not ready - here change some statemtens for new va_dev_ad_de_v and cr_aktpol_v
    # sql_statement = """
    #    select de_id, stats_mode(de_name_kurz) as GA_NAME, stats_mode(sprache_cdi) as GA_SPRACHE, 
    #    stats_mode(de_typ_cddev) as de_typ_cddev
    #    from `axa-ch-datalake-analytics-dev.various.va_dev_ad_de_v`
    #    where de_id in (select distinct org_nl_ga_b
    #                    from `axa-ch-datalake-analytics-dev.contract.cr_aktpol_v`
    #                    from tdbmk.agr_aktpol_v
    #                    where cor_stichtag_yyyymm = (select max(cor_stichtag_yyyymm) 
    #                    from `axa-ch-datalake-analytics-dev.contract.cr_aktpol_v`)
    #                    and org_nlel_kanal_b in ('AD', 'DIREKT'))
    #    group by de_id"""

    # df_dict = sf.sql_getdf(sql_statement, con_tdb, column_lower = False)

    sql_statement = """
    with dev_ad_de as 
    (select de_id, de_name_kurz as GA_NAME, sprache_cdi as GA_SPRACHE, de_typ_cddev, count(de_id) AS counts
     from `axa-ch-data-engineering-dev.various.va_dev_ad_de`
     where de_id in (select distinct GA_NL_B
                        from `axa-ch-data-engineering-dev.contract.cr_aktpol_m`
                        where stichtag = (select max(stichtag) from `axa-ch-data-engineering-dev.contract.cr_aktpol_m`)
                        and kanal_b in ('AD', 'DIREKT'))
     group by de_id, ga_name, ga_sprache, de_typ_cddev)
     , ranked as (select de_id, ga_name, ga_sprache, de_typ_cddev, 
     ROW_NUMBER() OVER (PARTITION BY de_id ORDER BY counts DESC) rank from dev_ad_de)
     select DE_ID, GA_NAME, GA_SPRACHE, DE_TYP_CDDEV from ranked where rank = 1
     """
    # change by original va_dev_ad_de

    df_dict = client.query(sql_statement).to_dataframe().drop_duplicates()
    # df_dict = client_eng.query(sql_statement).to_dataframe().drop_duplicates()
    #df_dict.head()

    # df_dict = sf.sql_getdf(sql_statement, con_tdb, column_lower = False)

    df_dict = df_dict.rename(columns = {'DE_ID': 'GA_ID'}).set_index('GA_ID')
    df_dict = df_dict[['GA_SPRACHE']]
    # df_dict.head()

    ga_dict = df_dict.to_dict('index')
    for key, value in ga_dict.items():
        ga_dict[key] = value.get('GA_SPRACHE')

    prospect_sharekg = 0.05
    #ga_dict

    # 20190429*gep hier geht es erstmal weiter #0.5 Initialisierung Kampagnen-Objekt campaign = cm.Campaign
    #0.5 Initialisierung Kampagnen-Objekt
    campaign = cm.Campaign(campaign_id              = 80017,
                        campaign_name            = "Bindexis Bauausschreibungen", 
                        campaign_manager         = ["thomas.knell@axa-winterthur.ch"],
                        campaign_techsupport     = ["tobias.ippisch@axa-winterthur.ch",
                                                    "natascha.spindler@axa-winterthur.ch",
                                                    "IMCEASMS-0041799422212@sms.wgr"],                  
                        campaign_sharekg         = "Permanent",
                        campaign_channelsplit    = {"AD": 1.0},                                                       
                        campaign_channelsplitvar = None,               
                        campaign_startdate       = "16.10.2017",
                        campaign_enddate         = "31.12.2025",
                        campaign_lineofbusiness  = "NL",
                        campaign_pathdata        = path_data, 
                        campaign_trackausschluss = True)


    #1   Ziehung Rohdaten
    #1.1 Ziehung Grunddaten Bauausschreibungen (Projekte, Kontakte, Objekte)
    #print("1.1")

    # sql_statement = """
    #         select *
    #         from fbtdbmk.bindexis_bau_projects
    #         where PROJECT_INRESEARCH = 0
    #         and DATE_INSERTION > to_date('{0}', 'dd.mm.yyyy')""".format(campaign.campaign_timelastrun.strftime("%d.%m.%Y"))

    #     df_projects = sf.sql_getdf(sql_statement, con_tdb, column_lower = False)  


    sql_statement = """SELECT * FROM `axa-ch-datalake-analytics-dev.BINDEXIS.bindexis_bau_projects2`
                    where PROJECT_INRESEARCH = 0 
                    and ADDRESS_COUNTRY = "CH" 
                    and DATE_INSERTION > '{0}' """.format(campaign_timelastrun.strftime("%Y-%m-%d")) 

    df_projects = client.query(sql_statement).to_dataframe()

    #print(df_projects.PROJECT_ID.count())

    print("campaign_timelastrun: " + campaign_timelastrun.strftime("%d.%m.%Y"))

    #df_projects.count()

    sql_statement = """CREATE OR REPLACE TABLE `axa-ch-datalake-analytics-dev.temp_da.tmp_bau_projects`
    OPTIONS(  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 120 MINUTE)) AS
    SELECT distinct PROJECT_ID
    FROM `axa-ch-datalake-analytics-dev.BINDEXIS.bindexis_bau_projects2`
    where PROJECT_INRESEARCH = 0 and ADDRESS_COUNTRY = "CH" 
    and DATE_INSERTION > '{0}' """.format(campaign_timelastrun.strftime("%Y-%m-%d")) 

    # if table does not exist please wait until it is created
    query_job = client.query(sql_statement)
    query_job.result()  # Waits for query to finish job.result()

    #df_projects = df_projects[df_projects.ADDRESS_COUNTRY == "CH"]

    contact_dict = {"CONTACT_TYPE":         "ORG_TYPE", 
                    "CONTACT_ID":           "ORG_ID",  
                    "CONTACT_GENDER":       "PERSON_GENDER", 
                    "CONTACT_FIRSTNAME":    "PERSON_FIRSTNAME", 
                    "CONTACT_LASTNAME":     "PERSON_LASTNAME", 
                    "CONTACT_ORGANIZATION": "ORG_NAME", 
                    "CONTACT_STREET1":      "ORG_STREET1", 
                    "CONTACT_STREET2":      "ORG_STREET2",              
                    "CONTACT_STREET3":      "ORG_STREET3",              
                    "CONTACT_POSTALCODE":   "ORG_POSTALCODE",  
                    "CONTACT_CITY":         "ORG_CITY",  
                    "CONTACT_COUNTRY":      "ORG_COUNTRY",
                    "CONTACT_PHONE1":       "ORG_PHONE",
                    "CONTACT_PHONE2":       "PERSON_PHONE",
                    "CONTACT_PHONE3":       "PERSON_MOBILE",
                    "CONTACT_EMAIL1":       "ORG_EMAIL",
                    "CONTACT_EMAIL2":       "PERSON_EMAIL",
                    "CONTACT_WEB":          "ORG_WEB",
                    "CONTACT_PERSON_ID":    "PERSON_ID"}

    # 20190429*gep - hier weiter mit join auf df_projects auf df_buildings

    # df_contacts = sf.sql_leftjoin(df = df_projects[["PROJECT_ID"]].drop_duplicates(), connection = con_tdb,
    #                 schema = "fbtdbmk", tablename = "bindexis_bau_contacts", key = "PROJECT_ID",
    #                 attributes = contact_dict.values(), 
    #                 column_lower = False) 

    sql = """
        SELECT 
        a.PROJECT_ID, 
        b.ORG_ID, b.PERSON_GENDER, b.PERSON_FIRSTNAME, b.PERSON_LASTNAME, b.ORG_NAME, b.ORG_TYPE, b.ORG_STREET1, b.ORG_STREET2, 
        b.ORG_STREET3, b.ORG_POSTALCODE, b.ORG_CITY, b.ORG_COUNTRY, b.ORG_PHONE, b.PERSON_PHONE, b.PERSON_MOBILE, b.ORG_EMAIL, 
        b.PERSON_EMAIL, b.ORG_WEB, b.PERSON_ID
        FROM       `axa-ch-datalake-analytics-dev.temp_da.tmp_bau_projects` a
        left join `axa-ch-datalake-analytics-dev.BINDEXIS.bindexis_bau_contacts2` b
        on a.PROJECT_ID = b.PROJECT_ID
    """

    df_contacts = client.query(sql).to_dataframe()

    sql = """SELECT a.PROJECT_ID, b.BUILDING_TYPE, b.BUILDING_DEVELOPMENT 
        FROM       `axa-ch-datalake-analytics-dev.temp_da.tmp_bau_projects` a    
        LEFT JOIN  `axa-ch-datalake-analytics-dev.BINDEXIS.bindexis_bau_buildings2` b
        on a.PROJECT_ID = b.PROJECT_ID"""

    # change by `axa-ch-datalake-analytics-dev.BINDEXIS.bindexis_bau_buildings2`

    df_buildings = client.query(sql).to_dataframe()

    #1.2 Aufbereitung Projektdaten
    print("1.2")
    aux_dict = {"PROJECT_TITLE":      "",   "PROJECT_DESCRIPTION": "",   "PROJECT_VALUE":    0,   
                "PROJECT_APARTMENTS":  0,   "PROJECT_PARCELID":    "",   "ADDRESS_STREET1": "", 
                "ADDRESS_STREET2":    "",   "ADDRESS_STREET3":     "",   "ADDRESS_CITY":    ""}       

    for i in aux_dict.keys(): df_projects[i] = df_projects[i].fillna(aux_dict[i])

    #df_projects.head()

    #1.3 Aufbereitung Kontaktdaten
    print("1.3")
    #Gruppierung Umformatierung Daten + Spaltenbereinigung
    for i in contact_dict.keys(): df_contacts[i] = df_contacts[contact_dict[i]].fillna("")

    aux_list = [i for i in contact_dict.keys()] + ["PROJECT_ID"]
    df_contacts = df_contacts[aux_list].copy()
    # df_contacts.head()

    #Klassifikation P- & U-Kontakte    
    def check_commercial_contact(firstname, lastname, orgname, orgtype, personid, contactid):        
        if (firstname == "") & (lastname == ""): 
            return (orgname, "U", contactid) #Organisationskontakt ohne persönlichen Ansprachpartner
        elif (firstname in orgname) & (lastname in orgname) & (orgtype in ["Bauherr", "Bauherrenvertreter"]): 
            return ("", "P", personid) #Privatpersonen als Organisation benannt (Bauherr, Bauherrenvertreter)
        elif (firstname in orgname) & (lastname in orgname) & (orgtype not in ["Bauherr", "Bauherrenvertreter"]): 
            return ("", "U", personid) #Privatpersonen als Organisation benannt (nicht Bauherr, Bauherrenvertreter)        
        else:
            return (orgname, "U", contactid) #Organisationskontakt mit persönlichem Ansprechpartner

    df_contacts["AUX"] = df_contacts.apply(lambda row: check_commercial_contact( \
                         row.CONTACT_FIRSTNAME, row.CONTACT_LASTNAME, row.CONTACT_ORGANIZATION, 
                         row.CONTACT_TYPE, row.CONTACT_PERSON_ID, row.CONTACT_ID), axis = 1)

    df_contacts["CONTACT_ORGANIZATION"] = df_contacts.AUX.apply(lambda x: x[0])         
    df_contacts["CONTACT_ORGTYPE"]      = df_contacts.AUX.apply(lambda x: x[1])    
    df_contacts["CONTACT_ID"]           = df_contacts.AUX.apply(lambda x: x[2])  
    df_contacts["CONTACT_ORGTYPE_NUM"]  = df_contacts.CONTACT_ORGTYPE.map({"P": 1, "U": 2})     

    #Zusammenführung Telefon- & Email-Adressen
    df_contacts["CONTACT_EMAIL1"] = df_contacts.apply(lambda row: row.CONTACT_EMAIL2 if row.CONTACT_EMAIL1 == "" else row.CONTACT_EMAIL1, axis = 1)
    df_contacts["CONTACT_EMAIL2"] = df_contacts.apply(lambda row: "" if row.CONTACT_EMAIL1 == row.CONTACT_EMAIL2 else row.CONTACT_EMAIL2, axis = 1)

    def tel_collector(tel1, tel2, tel3):
        tel_list = []
        for i in [tel1, tel2, tel3]:
            if i != "": tel_list += [i]
        return tel_list

    df_contacts["AUX"] = df_contacts.apply(lambda row: tel_collector(row["CONTACT_PHONE1"], row["CONTACT_PHONE2"], row["CONTACT_PHONE3"]), axis = 1)

    for i in range(3): 
        df_contacts["CONTACT_PHONE" + str(i+1)] = df_contacts.AUX.apply(lambda x: x[i] if len(x) >= i+1 else "")

    #1.4 Aufbereitung Objektdaten
    print("1.4")
    #Aufbereitung Development Type (Dummyfizierung)
    development_list = ["Neubau", "Umbau", "Anbau", "Abbruch", "Äussere Veränderungen"]

    aux_df = pd.DataFrame(df_buildings.groupby("PROJECT_ID").BUILDING_DEVELOPMENT.unique())

    for i in development_list:
        aux_df["BUILDING_DEVELOPMENT_" + str(i).replace(" ", "_").upper()] = aux_df.BUILDING_DEVELOPMENT.apply(lambda x: 1 if i in x else 0)

    df_buildings = df_buildings.merge(aux_df.drop("BUILDING_DEVELOPMENT", axis = 1), how = "left", left_on = "PROJECT_ID", right_index = True)
    df_buildings = df_buildings.drop("BUILDING_DEVELOPMENT", axis = 1)

    # df_buildings.head()

    #Aufbereitung Building Type allgemein (Dummyfizierung)
    type_list = ["Verkehrsanlagen", "Freizeit, Sport, Erholung", "Wohnen (bis 2 Wohneinheiten)",
                 "Wohnen (ab 3 Wohneinheiten)",  "Technische Anlagen", "Industrie und Gewerbe",
                  "Land- und Forstwirtschaft", "Unterricht, Bildung und Forschung",
                  "Kultur und Geselligkeit", "Gastgewerbe und Fremdenverkehr", "Handel und Verwaltung", 
                  "Kultus", "Fürsorge und Gesundheit", "Militär- und Schutzanlagen", 
                  "Justiz und Polizei"]

    aux_df = df_buildings[df_buildings.BUILDING_TYPE.notnull()][["PROJECT_ID", "BUILDING_TYPE"]].copy()

    for i in type_list:
        aux_str = re.sub('[()-, ]', '', str(i)).upper()
        aux_df["BUILDING_TYPE_" + aux_str] = aux_df.BUILDING_TYPE.apply(lambda x: 1 if i in x else 0)
        aux_df["BUILDING_TYPE"] = aux_df.BUILDING_TYPE.apply(lambda x: x.replace(i + "-", ""))
        aux_df["BUILDING_TYPE_" + aux_str] = aux_df.groupby("PROJECT_ID")["BUILDING_TYPE_" + aux_str].transform("max") 

    df_buildings = df_buildings.merge(aux_df.drop("BUILDING_TYPE", axis = 1).drop_duplicates("PROJECT_ID"), how = "left", on = "PROJECT_ID")

    #Aufbereitung Building Type spezifisch (Concatenation)   
    aux_df = pd.DataFrame(aux_df.groupby("PROJECT_ID")["BUILDING_TYPE"].apply(lambda x: " / ".join(set(x))))

    df_buildings = df_buildings.drop("BUILDING_TYPE", axis = 1).merge(aux_df, how = "left", left_on = "PROJECT_ID", right_index = True)

    #Deduplizierung & Feature Engineering
    df_buildings = df_buildings.drop_duplicates("PROJECT_ID").rename(columns = {"BUILDING_TYPE": "BUILDING_DETAIL"})

    #Befüllung Leerwerte
    aux_list = [i for i in df_buildings.columns if "BUILDING_DEVELOPMENT" in i] + \
               [i for i in df_buildings.columns if "BUILDING_TYPE" in i]

    for i in aux_list: df_buildings[i].fillna(0, inplace = True)

    #Anschlüsselung an df_projects
    df_projects = df_projects.merge(df_buildings, how = "left", on = "PROJECT_ID")

    #df_projects.head()

    # prepare df_contacts for later request as a temporary bigQuery table

    aux_list = ["CONTACT_ID",         "CONTACT_EMAIL1",       "CONTACT_EMAIL2", 
                "CONTACT_PHONE1",     "CONTACT_PHONE2",       "CONTACT_PHONE3",
                "CONTACT_FIRSTNAME",  "CONTACT_LASTNAME",     "CONTACT_STREET1", 
                "CONTACT_POSTALCODE", "CONTACT_ORGANIZATION", "CONTACT_ORGTYPE_NUM"]

    df_contacts_2 = df_contacts[aux_list].drop_duplicates("CONTACT_ID")
    # df_contacts_2.head()

    # 2. loading a dataframe into new bq table (does not exist before), should be temporary for 10 minutes
    dataset_ref = client.dataset('temp_da', project='axa-ch-datalake-analytics-dev')
    table_ref = dataset_ref.table('tmp_contacts')
    #expiration = datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=10)
    #table_ref.expires = expiration #  seems to have an effect, but also no error when processing next step

    # client.load_table_from_dataframe(df_tmp_builds, table_ref, expiration, project='axa-ch-datalake-analytics-dev').result()
    # result() waits until tables is loaded completely
    client.load_table_from_dataframe(df_contacts_2, table_ref,  project='axa-ch-datalake-analytics-dev').result()

    # call function for setting expiration time otherwise never expires
    # at creation step 2 --> qestion for google engineers
    tmp_table_expiration(table_ref, 120)

    #1.5 Partner-Matching (exakt) 
    print("1.5")    
    sql_statement = """
        select e.CONTACT_ID, d.PART_NR, 'EXAKT' as MATCH_TYPE, 100 as MATCH_SCORE, f.CONTACT_CUSTOMER
        from 
            (select a.*, b.EMAIL_ADR_1, c.TEL_NR_KOMPL
            from 
                    (select part_nr, gpart_typ_cdgpd, vname, nname, name_nnp_zeile1, plz,
                     CONCAT(rtrim(ltrim(name_nnp_zeile1)),' ',rtrim(ltrim(name_nnp_zeile2))) AS name_nnp ,
                     CONCAT(rtrim(ltrim( replace(STR_NAME, 'str.', 'strasse'))), ' ', rtrim(ltrim(haus_nr_kompl))) as strasse 
                     from `axa-ch-data-engineering-dev.customer.cs_partner`
                     where ersi_dat = '9999-12-31'
                     and kz_part_archvg_vgshn != '1') a 

            left outer join    
                    (select part_nr, email_adr_1
                     from `axa-ch-data-engineering-dev.customer.cs_email` 
                     where ersi_dat = '9999-12-31' and email_adr_1 is not NULL) b        
                     on a.PART_NR = b.PART_NR 

            left outer join    
                    (select part_nr, tel_nr_kompl
                     from `axa-ch-data-engineering-dev.customer.cs_telefon` 
                     where ersi_dat = '9999-12-31'
                     and tel_nr_kompl is not NULL) c        
                    on a.PART_NR = c.PART_NR ) d

        inner join

             `axa-ch-datalake-analytics-dev.temp_da.tmp_contacts` e  

        on (    d.TEL_NR_KOMPL in (e.CONTACT_PHONE1, e.CONTACT_PHONE2, e.CONTACT_PHONE3) 
                and d.GPART_TYP_CDGPD = cast(e.CONTACT_ORGTYPE_NUM as string))
        or (    d.EMAIL_ADR_1  in (e.CONTACT_EMAIL1, e.CONTACT_EMAIL2)
                and d.GPART_TYP_CDGPD = cast(e.CONTACT_ORGTYPE_NUM as string))
        or (    e.CONTACT_FIRSTNAME    = d.VNAME
                and e.CONTACT_LASTNAME     = d.NNAME
                and e.CONTACT_STREET1      = d.STRASSE
                and e.CONTACT_POSTALCODE   = d.PLZ
                and cast(e.CONTACT_ORGTYPE_NUM as string)  = d.GPART_TYP_CDGPD)
        or (    e.CONTACT_ORGANIZATION = d.NAME_NNP_ZEILE1
                and e.CONTACT_STREET1      = d.STRASSE
                and e.CONTACT_POSTALCODE   = d.PLZ                           
                and cast(e.CONTACT_ORGTYPE_NUM as string)  = d.GPART_TYP_CDGPD)
        or (    e.CONTACT_ORGANIZATION = d.NAME_NNP
                and e.CONTACT_STREET1      = d.STRASSE
                and e.CONTACT_POSTALCODE   = d.PLZ                           
                and cast(e.CONTACT_ORGTYPE_NUM as string)  = d.GPART_TYP_CDGPD)

        left outer join
                (select distinct PART_NR, 1 as CONTACT_CUSTOMER 
                 from `axa-ch-data-engineering-dev.contract.cr_aktpol_m`
                 where stichtag = 
                 (select max(distinct stichtag) from `axa-ch-data-engineering-dev.contract.cr_aktpol_m`)) f

                 on d.PART_NR = f.PART_NR"""    

    df_temp1 = client.query(sql_statement).to_dataframe()        

    # change by original cs_email, cs_partner, cs_telefon, cr_aktpol_m

    df_temp1.head()

    #aux_list = ["CONTACT_ID",         "CONTACT_EMAIL1",       "CONTACT_EMAIL2", 
    #            "CONTACT_PHONE1",     "CONTACT_PHONE2",       "CONTACT_PHONE3",
    #            "CONTACT_FIRSTNAME",  "CONTACT_LASTNAME",     "CONTACT_STREET1", 
    #            "CONTACT_POSTALCODE", "CONTACT_ORGANIZATION", "CONTACT_ORGTYPE_NUM"]

    # df_temp1 = sf.sql_getdf(sql_statement, con_tdb, {'df_contacts': df_contacts[aux_list].drop_duplicates("CONTACT_ID")}, column_lower = False)                       


    #Datenbereinigung 1: Wenn >1 Kontaktkriterien gleiche PART_NR ergeben dann nur 1 Eintrag benötigt    
    df_temp1 = df_temp1.drop_duplicates(["CONTACT_ID", "PART_NR"])

    #Datenbereinigung 2: Kunden mit aktiver Vertragsbeziehung priorisiert, langjährigere Kunden priorisiert
    df_temp1["CONTACT_CUSTOMER"] = df_temp1.CONTACT_CUSTOMER.fillna(0)
    df_temp1 = df_temp1.sort_values(["CONTACT_ID", "CONTACT_CUSTOMER", "PART_NR"], ascending = [True, False, True]).drop_duplicates("CONTACT_ID")
    #df_temp1.head()

    #1.6 Partner-Matching (fuzzy) - hier folgt normalerweise fuzzy matching - wurde hier erstmal weggelassen

    #Datenbereinigung 2: Kunden mit aktiver Vertragsbeziehung priorisiert, Kunden mit besserem Match_Score priorisiert
    #d f_temp2["CONTACT_CUSTOMER"] = df_temp2.CONTACT_CUSTOMER.fillna(0)
    # df_temp2 = df_temp2.sort_values(["CONTACT_ID", "CONTACT_CUSTOMER", "MATCH_SCORE"], ascending = [True, False, False]).drop_duplicates("CONTACT_ID")

    # einfacher Trick - damit der Code läuft (ohne Fuzzy Matching) - einfach df_temp2 von df_temp1 kopieren
    #df_temp2 = df_temp1
    #df_temp2.head()

    #1.7 Zusammenführung mit df_contacts
    print("1.7")
    # 20190508*gep org df_contacts = df_contacts.merge(df_temp1.append(df_temp2), how = "left", on = "CONTACT_ID")

    # für Testzwecke nach Hybris nur identifizierte Customer schicken, keine Prospects, da diese dann im Hybris/CRM neu 
    # angelegt werden
    df_contacts = df_contacts.merge(df_temp1, how = "left", on = "CONTACT_ID")
    # Attention: for test purposes only real customers and no prospects 
    # otherwise new partner numbers will be created but makes no sense
    df_contacts = df_contacts[df_contacts['PART_NR'].notnull()]
    df_contacts.loc[df_contacts.astype(str).drop_duplicates().index]

    df_contacts["MATCH_SCORE"] = df_contacts.MATCH_SCORE.fillna(0)
    df_contacts["CONTACT_CUSTOMER"] = df_contacts.CONTACT_CUSTOMER.fillna(0)
    df_contacts["CONTACT_PARTNER"] = df_contacts.PART_NR.notnull().astype(int)

    #2   Zusammenführung Projekte & Kontakte
    #2.1 Bestimmung führender Partner für Lead (Bauherr)
    print("2.1")
    df_temp = df_contacts[df_contacts.CONTACT_TYPE.isin(["Bauherr", "Bauherrenvertreter", "Architekt / Planer", "Generalunternehmung"])].copy()
    df_temp["AUX_CONTACT_TYPE"] = df_temp.CONTACT_TYPE.map({"Bauherr": 1, "Bauherrenvertreter": 2, "Architekt / Planer": 4, "Generalunternehmung": 3})
    df_temp = df_temp.sort_values(["PROJECT_ID", "AUX_CONTACT_TYPE", "CONTACT_CUSTOMER", "MATCH_SCORE", "CONTACT_GENDER"], 
                                  ascending = [True, True, False, False, False]).drop_duplicates("PROJECT_ID")   

    aux_index = df_temp.index #Hilfsindex für Ausschluss dieser Rollen bei den aux_roles

    #2.2 Anschlüsselung führender Partner an Projekt (und Entfernen von Projekte ohne führenden Partner)
    print("2.2")
    df_projects = df_projects.merge(df_temp, on = "PROJECT_ID", how = "inner") 

    #Projekte mit Bauherren im Ausland werden ausgeschlossen
    df_projects["AUX_CH"] = df_projects.apply(lambda row: 1 if ((row.CONTACT_TYPE == "Bauherr") & (row.CONTACT_COUNTRY != "CH")) else 0, axis = 1)
    df_projects = df_projects[df_projects.AUX_CH == 0]
    del df_projects["AUX_CH"]

    #2.3 Fokus auf 1 Lead pro CONTACT_ID & Tag (Architektur-Defizit Pilot-Schnittstelle)
    print("2.3")
    df_projects = df_projects.drop_duplicates("CONTACT_ID") 

    #2.4 Anschlüsselung zusätzlicher Rollen an Projekt
    print("2.4")
    df_auxroles = df_contacts.copy()

    auxroles_list = [[["Bauherr"],                                "ADD_BAUHERR_"],
                     [["Bauherrenvertreter"],                     "ADD_BAUHERRENVERTRETER_"],
                     [["Architekt / Planer"],                     "ADD_ARCHITEKT_"],
                     [["Bauingenieur"],                           "ADD_BAUINGENIEUR_"], 
                     [["Generalunternehmung", "Bauunternehmung"], "ADD_GENERALUNTERNEHMUNG_"]]

    aux_list = ["CONTACT_ID", "CONTACT_GENDER", "CONTACT_FIRSTNAME", "CONTACT_LASTNAME",
                "CONTACT_ORGANIZATION", "CONTACT_STREET1", "CONTACT_POSTALCODE", "CONTACT_CITY",
                "CONTACT_PHONE1", "CONTACT_PHONE2", "CONTACT_EMAIL1",  "CONTACT_ORGTYPE", 
                "CONTACT_CUSTOMER", "CONTACT_PARTNER", "PROJECT_ID", "PART_NR"]

    for i in auxroles_list:
        df_temp = df_auxroles[df_auxroles.CONTACT_TYPE.isin(i[0])]
        df_temp = df_temp.sort_values(["PROJECT_ID", "CONTACT_CUSTOMER", "MATCH_SCORE", "CONTACT_GENDER"], 
                                        ascending = [True,  False, False, False]).drop_duplicates("PROJECT_ID")[aux_list]

        #Umbenennen Felder in aux_list
        df_temp = df_temp.rename(columns = dict(zip(aux_list, [j.replace("CONTACT_", i[1]).replace("PART_NR", i[1] + "PART_NR") for j in aux_list])) )

        #Anschlüsselung an df_projects
        df_projects = df_projects.merge(df_temp, how = "left", on = "PROJECT_ID")

        #Befüllung Leerwerte (für spätere Freitext-Generierung)
        exclude_list = ["PART_NR", "CONTACT_ID", "CONTACT_CUSTOMER", "CONTACT_PARTNER", "PROJECT_ID"]
        for j in [k for k in aux_list if k not in exclude_list]:
            aux_val = j.replace("CONTACT_", "")
            df_projects[i[1] + aux_val] = df_projects[i[1] + aux_val].fillna("").astype(str)           

        df_projects[i[1] + "PART_NR"] = df_projects[i[1] + "PART_NR"].apply(lambda x: "" if pd.isnull(x) else str(int(x)))

        #Flagging Verfügbarkeit einzelner Rollen
        df_projects[i[1] + "AVAILABLE"] = 0
        aux_index = df_contacts[df_contacts.CONTACT_TYPE.isin(i[0])].PROJECT_ID.unique()
        aux_index = df_projects[df_projects.PROJECT_ID.isin(aux_index)].index
        df_projects.loc[aux_index, i[1] + "AVAILABLE"] = 1

    #3   Feature Engineering
    #3.1 Anschlüsselung GA- & Kanal-Information
    print("3.1")
    df_projects["GA"] = df_projects.ADDRESS_POSTALCODE.map(dict(zip(plz_df.PLZ, plz_df.GA)))
    df_projects["GA_LANGUAGE"] = df_projects["GA"].apply(lambda x: ga_dict.get(x, "DE"))
    df_projects["KANAL"] = "AD"
    df_projects["CAMPAIGN_NAME"] = "80017_Bindexis"

    #3.2 Trennung Festnetz- & Mobilnummer (für Schnittstelle)
    print("3.2")
    def tel_identifier(row, tel_type):
        tel_list = [row["CONTACT_PHONE{}".format(j)] for j in range(1,4)] 
        mobile_list = ["+4175", "+4176", "+4177", "+4178", "+4179"]

        if tel_type == "mobile": aux_list = [i for i in tel_list if i[:5]     in mobile_list]
        else:                    aux_list = [i for i in tel_list if i[:5] not in mobile_list]   

        if len(aux_list) > 0: return aux_list[0]
        else:                 return None 

    df_projects["AUX_PHONE_FIX"]    = df_projects.apply(lambda row: tel_identifier(row, "fix"), axis = 1)
    df_projects["AUX_PHONE_MOBILE"] = df_projects.apply(lambda row: tel_identifier(row, "mobile"), axis = 1)

    print("3.3")
    df_projects["AUX_STREET"]      = df_projects.CONTACT_STREET1.apply(lambda x: aux.split_housenumber_street(x)[0])
    df_projects["AUX_HOUSENUMBER"] = df_projects.CONTACT_STREET1.apply(lambda x: aux.split_housenumber_street(x)[1])

    #3.4 Generierung Freitext-Feld
    print("3.4")
    def freitext_generator(row):
        sprach_dict = {"PROJEKT":             {"DE": "PROJEKT", "FR": "PROJET", "IT": "PROGETTO"},
                       "BESCHREIBUNG":        {"DE": "BESCHREIBUNG", "FR": "DESCRIPTION", "IT": "DESCRIZIONE"},
                       "PARZELLE":            {"DE": "PARZELLE", "FR": "PARCELLE", "IT": "PARCELLA"},
                       "ADRESSE":             {"DE": "ADRESSE", "FR": "ADRESSE", "IT": "INDIRIZZO"},
                       "BAUWERT":             {"DE": "BAUWERT", "FR": "VALEUR DE CONSTRUCTIION", "IT": "VALORE DI COSTRUZIONE"},
                       "ANZ. APARTMENTS":     {"DE": "ANZ. APARTMENTS", "FR": "NOMBRE D’APPARTEMENTS", "IT": "NUMERO DI APPARTAMENTI"},
                       "ZUSÄTZL.":            {"DE": "ZUSÄTZL.", "FR": "SUPPLÉMENTAIRE", "IT": "ULTERIORE"},
                       "BAUHERR":             {"DE": "BAUHERR", "FR": "MAÎTRE D'OUVRAGE", "IT": "COMMITTENTE DELL'OPERA"},
                       "BAUHERRENVERTRETER":  {"DE": "BAUHERRENVERTRETER", "FR": "REPRÉSENTANT DU MAÎTRE D'OUVRAGE", "IT": "RAPPRESENTANTE DEL COMMITTENTE"},
                       "ARCHITEKT":           {"DE": "ARCHITEKT", "FR": "ARCHITECTE", "IT": "ARCHITETTO"},
                       "BAUINGENIEUR":        {"DE": "BAUINGENIEUR", "FR": "INGÉNIEUR CIVIL", "IT": "INGEGNERE CIVILE"},
                       "GENERALUNTERNEHMUNG": {"DE": "GENERALUNTERNEHMUNG", "FR": "ENTREPRISE GÉNÉRALE", "IT": "IMPRENDITORE GENERALE"},
                       "PHONE1":              {"DE": "TELEFON", "FR": "TELEFON", "IT": "TELEFON"},
                       "EMAIL1":              {"DE": "EMAIL", "FR": "EMAIL", "IT": "EMAIL"},
                       "PART_NR":             {"DE": "PART_NR", "FR": "PART_NR", "IT": "PART_NR"}}

        #Basisinformationen Objekt
        x = """{0}: {1} 
        {3} 
        {5}""".format(sprach_dict["PROJEKT"][row.GA_LANGUAGE], 
                   row.PROJECT_TITLE,
                   sprach_dict["BESCHREIBUNG"][row.GA_LANGUAGE],
                   row.PROJECT_DESCRIPTION,
                   sprach_dict["PARZELLE"][row.GA_LANGUAGE],
                   row.PROJECT_PARCELID)

        #Adresse
        aux_address = (row.ADDRESS_STREET1 + " " + row.ADDRESS_STREET2 + " " + row.ADDRESS_STREET3).rstrip()
        aux_address += ", " + str(row.ADDRESS_POSTALCODE) + " " + row.ADDRESS_CITY
        x += "{0}: {1}\\r\\n".format(sprach_dict["ADRESSE"][row.GA_LANGUAGE], aux_address) 

        if row.PROJECT_VALUE > 0:
            aux_val = locale.format("%d", row.PROJECT_VALUE, grouping = True)
            x += "{0}: {1} CHF\\r\\n".format(sprach_dict["BAUWERT"][row.GA_LANGUAGE], aux_val)

        if row.PROJECT_APARTMENTS > 0: 
            aux_val = str(row.PROJECT_APARTMENTS)
            x += "{0}: {1}\\r\\n".format(sprach_dict["ANZ. APARTMENTS"][row.GA_LANGUAGE], aux_val)           

        #Zusätzliche Rollen
        for i in ["ADD_BAUHERR_", "ADD_BAUHERRENVERTRETER_", "ADD_ARCHITEKT_", "ADD_BAUINGENIEUR_", "ADD_GENERALUNTERNEHMUNG_"]:                        
            if pd.notnull(row[i + "ID"]): 
                #Konkatinieren Name der Organisation / Person
                aux_name = "{0} {1} {2}".format(row[i + "ORGANIZATION"], row[i + "FIRSTNAME"], row[i + "LASTNAME"])
                aux_name = aux_name.strip()

                #Konkatinieren PART_NR und Kontaktinformationen
                aux_contact = []
                for z in ["PART_NR", "PHONE1", "EMAIL1"]: 
                    if row[i + z] != "": aux_contact += ["{0}: {1}".format(sprach_dict[z][row.GA_LANGUAGE], row[i + z])]              

                if aux_contact == []: aux_contact = ""
                else: aux_contact = "({0})".format(", ".join(aux_contact))

                #Konkatinieren Adressinformation
                aux_address = "{0}, {1} {2}".format(row[i + "STREET1"], row[i + "POSTALCODE"], row[i + "CITY"])
                aux_address = aux_address.strip()

                x += "{0} {1}: {2} {3}, {4}\\r\\n".format(sprach_dict["ZUSÄTZL."][row.GA_LANGUAGE], 
                                                          sprach_dict[i[4:-1]][row.GA_LANGUAGE],
                                                          aux_name, aux_contact, aux_address)

        return x.replace(";", ",").replace("\\r\\n", ", ")

    df_projects["BINDEXIS_AD_LEAD_INFO"] = df_projects.apply(lambda row: freitext_generator(row), axis = 1)

    #3.5 Generierung Felder für Hybris-Schnittstelle
    print("3.5")
    #Interaction
    df_projects["IAKTN_ART_CDYMKT"] = "2"                       
    df_projects["KOMKN_MEDIUM_CDYMKT"] = "3"                    
    df_projects["REF_BO_KLSFKN_CDU"] = "BO-BAUGES-ID-HYBRIS"    #TBD

    #Contact
    df_projects["IAKTN_KNTKT_REF_TYP_CDYMKT"] = "2"              
    df_projects["FCT_IAKTN_KNTKT_REF_ID"] = df_projects.PART_NR.apply(lambda x: str(int(x)) if pd.notnull(x) else "") 
    df_projects["FCT_IAKTN_KNTKT_REF_TYP_CDYMKT"] = df_projects.FCT_IAKTN_KNTKT_REF_ID.apply(lambda x: "" if x == "" else "4")        
    df_projects["GPART_TYP_CDGPD"] = df_projects.CONTACT_ORGTYPE.map({"P": "1", "U": "2"})
    df_projects["ANRED_ASHRFT_CDGPD"] = df_projects.CONTACT_GENDER.map({"Herr": "1", "Frau": "2"})
    df_projects["SEX_CDU"] = df_projects.CONTACT_GENDER.map({"Herr": "1", "Frau": "2"})
    df_projects["NAME_NNP_ZEILE1"] = df_projects.CONTACT_ORGANIZATION.str[:35]

    #4   Qualitätsfilterung Baugesuche & Deselektion   
    #4.1 Filterung auf bestehende Kunden
    df_projects = df_projects[df_projects.CONTACT_CUSTOMER == 1].copy()

    #4.2 Qualitäts-Ausschlüsse
    print("4.2")
    #A) Kategorisierung Projektkosten
    df_projects['EXCLUDE_LOWVALUE'] = df_projects.PROJECT_VALUE.apply(lambda x: 1 if ((x > 0) & (x <= 200000)) else 0)

    #B) Identifikation von Abbruchprojekten
    change_list = ['BUILDING_DEVELOPMENT_NEUBAU', 'BUILDING_DEVELOPMENT_UMBAU','BUILDING_DEVELOPMENT_ANBAU',
                   'BUILDING_DEVELOPMENT_ÄUSSERE_VERÄNDERUNGEN', 'BUILDING_DEVELOPMENT_ABBRUCH']        

    def building_develop_sum_1(summe, change):
        if ((summe == 1) & (change == 1)): return 1
        else:                              return 0                             

    df_projects['BUILDING_DEVELOPMENT_SUM'] = df_projects[change_list].sum(axis = 1)                          
    df_projects["EXCLUDE_ABBRUCH"] = df_projects.apply(lambda row: building_develop_sum_1(row["BUILDING_DEVELOPMENT_SUM"], row["BUILDING_DEVELOPMENT_ABBRUCH"]), axis = 1)    

    #C) Identifikation von relevanten Objekten
    def flag_irrelevant(x, rel, irrel):
        if any(i in x for i in irrel):
            if any(j in x for j in rel): return 0
            else:                        return 1
        else:                            return 0  

    df_projects["BUILDING_DETAIL"] = df_projects.BUILDING_DETAIL.fillna('unknown')                

    #Definition der Positiv- und Negativ-Liste der Objekte
    relevant_objects =      ['Arztpraxen und Ärztehäuser', 
                             'Atelier und Studio', 
                             'Ausstellungsbauten', 
                             'Autowerkstätten', 
                             'Bauernhäuser', 
                             'Betriebs- und Gewerbebauten', 
                             'Bürobauten', 
                             'Doppel-Einfamilienhäuser', 
                             'Einfamilienhaus-Siedlungen',
                             'Einfamilienhäuser', 
                             'Elektrische Verteilanlagen',
                             'Fitnesscenter',
                             'Heizzentralen, Fernwärmeanlagen und Kraftwerkbauten',
                             'Herbergen, Jugendherbergen und Massenunterkünfte',
                             'Hotel- und Motelbauten',
                             'Industriehallen',
                             'Industrielle Produktionsbauten',
                             'Kantinen',
                             'Kino-, Diskothek- und Saalbauten',
                             'Ladenbauten',
                             'Lager- und Umschlagplätze',
                             'Lagerhallen',
                             'Lebensmittelproduktion',
                             'Mehrfamilienhäuser',
                             'Mehrgeschossige Lagerbauten',
                             'Privatschwimmbäder, Jacuzzi, Wellness',
                             'Raststätten, Cafeterias, Tea-Rooms und Bars',
                             'Reihenhäuser',
                             'Reithallen',
                             'Restaurationsbetriebe',
                             'Schlachthöfe',
                             'Stallungen und landwirtschaftliche Produktionsanlagen',
                             'Supermärkte',
                             'Terrassenhäuser',
                             'Tiefgaragen und Unterniveaugaragen',
                             'Warenhäuser, Einkaufszentren und Showrooms',
                             'Wärme- und Kälteverteilanlagen',
                             'Wohlfahrtshäuser, Klubhäuser und Kulturzentren',
                             'Wohnungen']

    irrelevant_objects    = ['Abdankungshallen',
                             'Alterswohnheime', 
                             'Alterswohnungen, Alterssiedlungen',
                             'Aussenanlagen, Kinderspielplätze und Parkanlagen',
                             'Bahnhöfe und Bahnbetriebsbauten, Seilbahnstationen', 
                             'Banken, Postgebäude und Fernmeldegebäude', 
                             'Behelfswohnungen',
                             'Berghäuser',
                             'Berufs- und höhere Fachschulen', 
                             'Bibliotheken und Staatsarchive', 
                             'Bootshäuser',
                             'Burgen & Schlösser', 
                             'Busbahnhöfe, Zollanlagen und Wartehallen mit Diensträumen', 
                             'Campinganlagen',
                             'Casino', 
                             'Deponien',
                             'Feuerwehrgebäude',
                             'Flughafenbauten',
                             'Forschungsinstitute',
                             'Freizeitzentren und Jugendhäuser',
                             'Friedhofanlagen',
                             'Futterlagerräume, Treibhäuser und Silobauten',
                             'Garagen und Unterstände',
                             'Gartenhäuser', 
                             'Gemeindehäuser, Rathäuser und Regierungsgebäude',
                             'Gerichtsgebäude', 
                             'Gewächshäuser', 
                             'Hallen- und Freibäder',
                             'Heilbäder und Spezialinstitute',
                             'Heilpädagogische Schulen/Sonderschulen',
                             'Hochschulen und Universitäten',
                             'Kasernen', 
                             'Kehrichtverbrennungs- und Wiederaufbereitungsanlagen',
                             'Keller', 
                             'Kinder- und Jugendheime',
                             'Kinderhorte und Kindergärten',
                             'Kirchen und Kapellen',
                             'Kirchgemeindehäuser',
                             'Klöster',
                             'Klubhütten', 
                             'Kongresshäuser und Festhallen',
                             'Konzertbauten und Theaterbauten', 
                             'Krankenhäuser',
                             'Krematorien', 
                             'Lofts', 
                             'Mechanisierte Lager und Kühllager', 
                             'Militäranlagen und militärische Schutzanlagen', 
                             'Mittelschulen und Gymnasien',
                             'Museen und Kunstgalerien',
                             'Musikpavillons', 
                             'Öffentliche WC-Anlagen', 
                             'Öffentliche Zivilschutzanlagen', 
                             'Parkhäuser und Einstellhallen',
                             'Parkplätze und Abstellplätze', 
                             'Pavillons',
                             'Pflegeheime, Sanatorien und Rehabilitationszentren',
                             'Polizeieinsatzgebäude und Untersuchungsgefängnisse',
                             'Post- und Logistikterminale',
                             'Primar- und Sekundarschulen',
                             'Radio-, Fernseh- und Filmstudios', 
                             'Sammelstellen',
                             'Schuppen und Hütten',
                             'Silobauten und Behälter', 
                             'Sportanlagen, Turn- und Mehrzweckanlagen',
                             'Strafvollzugsanstalten', 
                             'Strassenverkehrsgebäude',
                             'Studenten- und Lehrlingswohnheime',
                             'Tagesheime und geschützte Werkstätten',
                             'Tankanlagen und Tankstellen', 
                             'Tierheime und Veterinärstationen',
                             'Tierspitäler',
                             'Tribünenbauten und Garderobengebäude', 
                             'Universitätskliniken',
                             'unknown',
                             'Verteilanlagen für Trinkwasser', 
                             'Verteilzentralen', 
                             'Verwaltungsgebäude und Rechenzentren',
                             'Wasseraufbereitungsanlagen', 
                             'Wellness', 
                             'Werkhöfe', 
                             'Wintergärten und Balkonverglasungen', 
                             'Zeughäuser',
                             'Zoologische und botanische Gärten, Tierhäuser']  

    df_projects['EXCLUDE_IRRELEVANT_OBJECT'] = df_projects.BUILDING_DETAIL.apply(lambda x: flag_irrelevant(x, relevant_objects, irrelevant_objects))                       

    #D) Identifikation von relevanten Subjekten     
    irrelevant_subjects = ['wärmepumpe', 'pompa di calore', 'termopompe', 'pompe à chaleur', 
                           'fenster', 'finestra', 'fenêtre', 
                           'dach', 'tetto', 'toit', 
                           'fassade', 'facciata', 'façade', 
                           'heizung', 'riscaldamento', 'chauffage', 
                           'kamin', 'caminetto', 'caminetti', 'cheminée', 
                           'wasser', 'acqua', ' eau', 
                           'sitzplatz', 'terrasse', 'terrazzo', 'sedile', 'banc', 
                           'balkon', 'balcone', 'balcon', 
                           'unterstand', 'rifugio', 'subalterno', 'abri', 'container', 
                           'wartung', 'manutenzione', 'mantenimento', 'entretien', 
                           'transformator', 'trasformatore', 'alimentatore', 'transformatrice', 'transformateur',
                           'leuchtkasten', 'postomat', 'box luce', 'caisson lumineux',
                           'pergola',
                           'sonnensegel', 'tenda', 'tende', 'banne',
                           'schaukel', 'altalene', 'balançoire',
                           'gartenhaus', 'casa estiva', "maison d'été",
                           'sichtschutz', 'vita privata', 'intimité',
                           'verglasung', 'vetri', 'vitrage',
                           'treppe', 'scala', 'escalier',
                           'windfang', 
                           'erdsonde', 
                           'spielplatz', 'terreno di gioco', 'cour de récréation',
                           'gewächshaus', 'serra', 'serre',
                           'zaun', 'recinto', 'clôture',
                           'whirlpool', 
                           'klimaanlage', 'aria condizionata', 'climatisation',
                           'bienenhaus', 'apiario', 'rucher']

    relevant_subjects = ['einfamilienhaus', 'single famiglia', 'casa monofamiliare', 'maison individuelle', 
                         'zweifamilienhaus', 'casa bifamiliare', 'deux maison familiale', 'maison double',
                         'dreifamilienhaus', 'casa trifamiliare', 'casa per tre famiglie', 'maison de trois familles', 'maison à trois logements',
                         'mehrfamilienhaus', 'palazzina', 'casa per immobili', 'maison appartement', "Immeuble d'habitation",
                         'wohnhaus', 'edificio residenziale', 'abitazione', "immeuble d'habitation", 'habitation', 
                         'gebäude', 'costruzione', 'edificio', 'bâtiment', 'immeuble', 
                         'wohnung', 'appartamento', 'alloggiamento', 'appartement', 
                         'spital', 'krankenhaus', 'klinik', 'ospedale', 'clinica', 'hôpital', 'clinique', 
                         'mehrzweck', 'multiuso', 'multifunzionale', 'polyvalent', 
                         'kongress', 'congresso', 'congrès', 
                         'lagerhalle', 'magazzino', 'deposito', 'entrepôt', 
                         'dachgeschoss', 'soffitta', 'piano mansarda', 'grenier', 
                         'dachaufstockung', 'dachaufbau', 'estensione tetto', 'alzare il tetto', 'surélever le toit', 'extension',
                         'photovoltaik', 'fotovoltaico', 'photovoltaïque', 'photovoltaique', 
                         'wasserreservoir', 'serbatoio di acqua', 'bacino idrico', "réservoir d'eau", 
                         'wasserversorgung', "approvvigionamento idrico", 'approvisionnement en eau', 'raccordement eau',
                         'pumpwerk', 'stazione di pompaggio', 'station de pompage',
                         'château', 'chateau', 'castello', 'chapiteau', 'niveau', 'wasserkraftwerk', 
                         'construction logement', 'constructione urbane']                       

    df_projects['EXCLUDE_IRRELEVANT_SUBJECT'] = df_projects.PROJECT_TITLE.apply(lambda x: flag_irrelevant(x.lower(), relevant_subjects, irrelevant_subjects))

    #E) Zusammenführung Ausschluskriterien
    def aggregate_objectsubject(obj, sub):
        if (obj == 0):
            if (sub == 1): return 1
            else:          return 0
        else:              return 1

    df_projects["EXCLUDE_TOTAL"] = df_projects.apply(lambda row: aggregate_objectsubject(row.EXCLUDE_IRRELEVANT_OBJECT, row.EXCLUDE_IRRELEVANT_SUBJECT), axis = 1)

    aux_index = df_projects[(df_projects.EXCLUDE_LOWVALUE == 1) | (df_projects.EXCLUDE_ABBRUCH == 1)].index
    df_projects.loc[aux_index, "EXCLUDE_TOTAL"] = 1

    df_projects["KEEP_TOTAL"] = df_projects.EXCLUDE_TOTAL.apply(lambda x: 1 - x)

    #4.2 Aufteilung Kunden / Prospects & Anspielung df_cust an Tranche
    print("4.2")    



    #type(campaign)
    # tranche.tranche_df.head()
    #for att in dir(tranche):
    #    print (att, getattr(tranche,att)) 

    #5   Kanalausspielung
    #5.1 Kanalzuordnung & Bildung Kontrollgruppe (für Propsects noch separat)
    print("5.1")
    df_projects["TRANCHE_KG"] = df_projects.PART_NR.apply(lambda x: 1 if random.random() < prospect_sharekg else 0)
    df_projects["TARGETGROUP"] = df_projects.TRANCHE_KG.map({1:"0", 0:"1"})
    # df_projects["TRANCHE_DATUM"] = tranche.tranche_timestamp
    # df_projects["TRANCHE_NUMMER"] = tranche.tranche_number
    df_projects["KANAL"] = df_projects.KEEP_TOTAL.apply(lambda x: "AD" if x == 1 else "AUSSCHLUSS")       
    df_projects["CHANNEL"] = "LEAD"

    tranche = cm.Tranche(campaign, df_projects, stage)

    tranche.tranche_df["TRANCHE_DATUM"] = tranche.tranche_timestamp
    tranche.tranche_df["TRANCHE_NUMMER"] = tranche.tranche_number

    #5.2 Aktualisierung Tranche & KPIs
    print("5.2")
    tranche.kpi_customers_identified     = df_projects.shape[0]
    tranche.kpi_customers_selected       = tranche.tranche_df.shape[0]
    tranche.kpi_size_targetgroup         = tranche.tranche_df[tranche.tranche_df.TRANCHE_KG == 0].shape[0]
    tranche.kpi_size_controlgroup        = tranche.tranche_df[tranche.tranche_df.TRANCHE_KG == 1].shape[0]

    #5.3 Aktualisierung NVP-Informationen auf Tranchen-Objekt
    print("5.3") 


    #5.4 Anspielung Pilotschnittstelle
    print("5.4")      
    interaction_mapping = {"iaktn_art_cdymkt":      "IAKTN_ART_CDYMKT", 
                           "komkn_medium_cdymkt":   "KOMKN_MEDIUM_CDYMKT", 
                           "ref_bo_klsfkn_cdu":     "REF_BO_KLSFKN_CDU",
                           "ref_bo_id":             "PROJECT_ID", 
                           "ntz_txt_lang":          "BINDEXIS_AD_LEAD_INFO"}

    contact_mapping = {"iaktn_kntkt_ref_id":             "CONTACT_ID",
                       "iaktn_kntkt_ref_typ_cdymkt":     "IAKTN_KNTKT_REF_TYP_CDYMKT",                                       
                       "fct_iaktn_kntkt_ref_id":         "FCT_IAKTN_KNTKT_REF_ID",
                       "fct_iaktn_kntkt_ref_typ_cdymkt": "FCT_IAKTN_KNTKT_REF_TYP_CDYMKT",
                       "gpart_typ_cdgpd":                "GPART_TYP_CDGPD",  
                       "name_nnp_zeile1":                "NAME_NNP_ZEILE1",       
                       "anred_ashrft_cdgpd":             "ANRED_ASHRFT_CDGPD",
                       "vname":                          "CONTACT_FIRSTNAME",
                       "nname":                          "CONTACT_LASTNAME",         
                       "sex_cdu":                        "SEX_CDU",
                       "spra_cdi_korrz":                 "GA_LANGUAGE",              
                       "email_adr":                      "CONTACT_EMAIL1",
                       "tel_nr_kompl":                   "AUX_PHONE_FIX",
                       "tel_nr_kompl_mobil":             "AUX_PHONE_MOBILE",
                       "str_name":                       "AUX_STREET",
                       "haus_nr_kompl":                  "AUX_HOUSENUMBER",     
                       "plz":                            "CONTACT_POSTALCODE",
                       "ort_name":                       "CONTACT_CITY",
                       "land_cdi":                       "CONTACT_COUNTRY"}

    nvp_mapping = {"CHANNEL":      "CHANNEL", 
                   "CAMPAIGNNAME": "CAMPAIGN_NAME", 
                   "TARGETGROUP":  "TARGETGROUP"}

    # FROM DATALAKE 1.0 - Achtung falsch, nicht nehmen, gemäss Sandor und Reto Haag sollten
    # die neuen Funktionen nehmen siehe soa_gc2 
    # wir benötigen noch ein Zertifikat --> Sandor 
    # wichtig - sicher sein, dass als Stage ACC übergeben wird, dann sollte die richtige URL 
    # für Hybris aus Standard in Standardfunktion soa_marketinginteraktiondatenpush_1
    # einzig der Parameter für das CERT-File muss neu evtl. mitgegeben werden, oder aber wir
    # überschreiben in der Funktion die aktuell Einstellung direkt

    #SOA service call 
    count_sel = df_projects.PROJECT_ID.count()

    if stage == "ACC":
        print ("count_sel: ", count_sel)
        print('nun folgt der SOA Aufruf und tranche.route_to_hybris ...')
    # 20190516*gep: näcshte Zeile als Trick damit es keinen Abbruch gibt, da keine echte Kanalaufspielung
        tranche.tranche_status = "3: Konstante"
        #tranche.route_to_hybris(interaction_mapping, contact_mapping, nvp_mapping)
        print("Ausgabe nach run tranche.route_to_hybris")

    sys.exit(0)

except Exception:
    #6   Exception Handling, Backup, Reporting
    #6.1 eMail-Benachrichtigung Exception
    print("6.1")
    print("hier normal Aufruf tranche.report_exception(traceback.format_exc()) - im GCP PoC nicht")
    tranche.report_exception(traceback.format_exc())

    sys.exit(1)
    
else:
    #6.2 Erstellung Backup 
    print("6.2")
    #Liste aller Spalten der Tranche zur Generierung der Variablenliste
    column_list = list(tranche.tranche_df.columns.values)
    
    #Variablenliste: Basisinformation" 
    var_list =  ["KANAL",  "TRANCHE_KG", "TRANCHE_NUMMER", "TRANCHE_DATUM", "GA", "PART_NR", 
                 "MATCH_TYPE", "MATCH_SCORE", "API_RESULT"]
    
    #Variablenliste: Projekt- & Kontaktinformationen
    for k in ["PROJECT_", "DATE_", "DETAIL_", "ADDRESS_", "CONTACT_"]:
        var_list += [i for i in column_list if i.startswith(k)]    
    
    #Variablenliste: Zusatzinformationen
    for k in ["BUILDING_", "ADD_BAUHERR_", "ADD_BAUHERRENVERTRETER_", "ADD_ARCHITEKT_", 
              "ADD_BAUINGENIEUR_", "ADD_GENERALUNTERNEHMUNG_"]:
        var_list += [i for i in column_list if i.startswith(k)]
    
    # print("6.2: hier eigentlich campaign.backup(tranche, var_list)")
    # campaign.backup(tranche, var_list)
    
# 20190520*gep workaround 62 Speicherung Zeit Parameter bei erfolgreichem Lauf
    if stage == "ACC": 
        try:
            campaign_timelastrun = datetime.datetime.now(pytz.timezone('Europe/Zurich'))
            filename = "{}/kampagne2.pkl".format(tempfile.gettempdir())
            with open(filename, 'wb') as fp: pickle.dump(campaign_timelastrun, fp)
            blob = bucket_cs.blob(path_data_va+'kampagne2.pkl')
            blob.upload_from_filename(filename)
            print("6.2 wa backup try")
        except:
            print("attention: except true")
        
        print(campaign_timelastrun)
    
    # workaround zu campaign.backup(tranche, var_list) rück schreiben des Zeitpunkts 
    # 'gs://axa-ch-raw-dev-dla/bindexis/data/various/kampagne.pkl' 
    # das rück schreiben erfolgt in der methode 

    #6.3 eMail-Benachrichtigung erfolgreicher Lauf
    #print("6.3: tranche.report_success()")
    # tranche.report_success()
    
    print("6.4: trigger run beendet")
    
    sys.exit(0)