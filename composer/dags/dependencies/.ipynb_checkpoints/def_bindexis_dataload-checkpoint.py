

def bindexis_dataload(user_bindexis, pw_bindexis):
    
    #0   Modul-Import & Parametrierung    
    #0.1 Modul-Import
    import os
    import pickle
    import requests
    import datetime
    import pytz
    import time
    import traceback
    import numpy as np
    import pandas as pd
    import xml.etree.ElementTree as ET
    import tempfile
    import logging
    from html import unescape
    from random import randint
    from google.cloud import bigquery
    from google.cloud import storage

    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    BUCKET = os.environ['BUCKET']
    PROJECT = os.environ['PROJECT']
    REGION = os.environ['REGION']


    #0.2 Set Stage
    stage = "DEV" #sf.platform_is_server("stage")

    #0.3 Pfadinformationen
    path_data_va = "bindexis/data/various/"
    path_data_input = "bindexis/data/input/"

    #0.4 Set timezone
    os.environ['TZ'] = 'Europe/Zurich'
    time.tzset()

    #1   Retrieve Bindexis Data
    #1.1 Access & Authentication Setup
    print("1.1")
    username      = user_bindexis
    password      = pw_bindexis
    time_now      = datetime.datetime.now(pytz.timezone('Europe/Zurich'))

    client_cs = storage.Client()
    bucket_cs = client_cs.get_bucket(BUCKET)
    try:
        filename = "{}/Parameter_TimeLastRun.pkl".format(tempfile.gettempdir())
        #blob = bucket_cs.blob(path_data_va+'Parameter_TimeLastRun.txt')
        #time_last_run = blob.download_as_string()
        #time_last_run = datetime.datetime.strptime(time_last_run.decode("utf-8")[:-6], "%Y-%m-%d %H:%M:%S.%f")
        blob = bucket_cs.blob(path_data_va+'Parameter_TimeLastRun.pkl')
        blob.download_to_filename(filename)
        with open(filename, 'rb') as fp: time_last_run = pickle.load(fp)
    except:
        time_last_run      = time_now + datetime.timedelta(days=-2)

    print(time_last_run)

    print("1.2")
    #file = "../certificates/sas_server_keystore.pem"
    data = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:soap="http://soap.bindexis.ch/">
               <soapenv:Header>
                  <soap:AuthHeader>
                     <!--Optional:-->
                     <soap:UserName>{0}</soap:UserName>
                     <!--Optional:-->
                     <soap:Password>{1}</soap:Password>
                  </soap:AuthHeader>
               </soapenv:Header>
               <soapenv:Body>
                  <soap:GetProjectSync>
                     <!--Optional:-->
                     <soap:dateToStart>{2}</soap:dateToStart>
                  </soap:GetProjectSync>
               </soapenv:Body>
            </soapenv:Envelope>""".format(username, password, time_last_run.strftime("%Y-%m-%dT%H:%M:%S"))

    response = requests.post(url     = "https://soap.bindexis.ch/projectsync.asmx",
                             headers = {'content-type': 'text/xml'},
   #                          cert    = file,
                             data    = data,
                             verify  = False)

    base_text = unescape(response.text)
    root = ET.fromstring(base_text[base_text.find("<?xml", 1): base_text.find("</GetProjectSyncResult>")])

    # 1.21 Save response.text to GCS
    blob = bucket_cs.blob(path_data_input+'bindexis_{}.txt'.format(time_now.strftime('%Y%m%d%H%M%S')))
    blob.upload_from_string(response.text)

    #1.3 Extraction Description Dictionary
    print("1.3")
    description_dict = {}

    for i in ["PlanningStage", "SizeUnit", "RoofType", "ConstructionMaterial", "ProjCladdingType", 
              "DevelopmentType", "TargetGroupType", "Employee", "JobFunction", "Gender", "BuildingType", 
              "HeatingType", "ProjectFinalUse"]:

        aux_dict = {}
        if i == "ProjectFinalUse": values = root.find("{0}".format(i))
        else:                      values = root.find("{0}Values".format(i))

        if i == "BuildingType":
            for j in values[1:]: aux_dict[j.find("ProId").text] = [j.find("ProDesc").text, j.find("ProParentId").text]     
        else: 
            for j in values: aux_dict[j.find("UtilId").text] = j.find("UtilDesc").text

        description_dict[i] = aux_dict


    #1.4 Definition of Extraction Configuration Files
    print("1.4")
    canton_dict = {'CH01': 'AG',     'CH02': 'AI',     'CH03': 'AR',     'CH04': 'BE',     'CH05': 'BL', 
                   'CH06': 'BS',     'CH08': 'FR',     'CH09': 'GE',     'CH10': 'GL',     'CH11': 'GR', 
                   'CH12': 'JU',     'CH13': 'LU',     'CH14': 'NE',     'CH15': 'NW',     'CH16': 'OW',     
                   'CH17': 'SG',     'CH18': 'SH',     'CH19': 'SO',     'CH20': 'SZ',     'CH21': 'TG',             
                   'CH22': 'TI',     'CH23': 'UR',     'CH24': 'VD',     'CH25': 'VS',     'CH26': 'ZG',     
                   'CH27': 'ZH'}  

    project_list =   [["PROJECT_ID",             "j.find('ProjId').text",                          "STRING"  ],
                      ["PROJECT_TITLE",          "j.find('ProjTitle').text",                       "STRING"  ],   
                      ["PROJECT_PARCELID",       "j.find('ProjOfficialId').text",                  "STRING"  ],  
                      ["PROJECT_LANGUAGE",       "j.find('ProjLanguageId').text[:2].upper()",      "STRING"   ],
                      ["PROJECT_DESCRIPTION",    "j.find('ProjDesc').text",                        "STRING"],
                      ["PROJECT_PLANNINGSTAGE",  "j.find('PlanningStage').text",                   "STRING"  ],
                      ["PROJECT_INRESEARCH",     "j.find('InResearch').text",                      "INTEGER"       ],                       
                      ["PROJECT_FINALUSE",       "j.find('ProjFinalUseId').text",                  "STRING"  ],
                      ["PROJECT_VALUE",          "j.find('ProjValue').text",                       "FLOAT"       ],
                      ["PROJECT_SIZE",           "j.find('ProjSize').text",                        "FLOAT"       ],
                      ["PROJECT_VOLUME",         "j.find('ProjVolume').text",                      "FLOAT"       ],
                      ["PROJECT_APARTMENTS",     "j.find('ProjApartments').text",                  "FLOAT"       ],
                      ["PROJECT_FLOORS",         "j.find('ProjFloors').text",                      "FLOAT"       ],
                      ["PROJECT_BUILDINGS",      "j.find('ProjBuildings').text",                   "FLOAT"       ],                 
                      ["PROJECT_NOTE",           "j.find('ProjNote').text",                        "STRING" ],
                      ["DATE_INSERTION",         "j.find('ProjInsertDate').text[:19]",             "timestamp"    ],
                      ["DATE_UPDATE",            "j.find('ProjLatestUpdate').text[:19]",           "timestamp"    ],
                      ["DATE_PUBLICATION",       "j.find('ProjConfirmDate').text[:19]",            "timestamp"    ],
                      ["DATE_STARTCONSTRUCTION", "j.find('ProjConstStartDate').text[:19]",         "timestamp"    ],
                      ["DATE_PLANNINGAPPROVAL",  "j.find('ProjPlanningApprovalDate').text[:19]",   "timestamp"    ],
                      ["DETAIL_ROOFTYPE",        "j.find('RoofType').text",                        "STRING"  ],
                      ["DETAIL_MATERIALS",       "j.find('ConstructionMaterials').text",           "STRING"  ],
                      ["DETAIL_CLADDING",        "j.find('CladdingType').text",                    "STRING"  ],
                      ["DETAIL_HEATING",         "j.find('HeatingType').text",                     "STRING"  ], 
                      ["DETAIL_SOLAR",           "j.find('ProjSolar').text",                       "INTEGER"       ], 
                      ["ADDRESS_STREET1",        "j.find('ProjAddress1').text",                    "STRING"  ],                                     
                      ["ADDRESS_STREET2",        "j.find('ProjAddress2').text",                    "STRING"  ], 
                      ["ADDRESS_STREET3",        "j.find('ProjAddress3').text",                    "STRING"  ],                   
                      ["ADDRESS_CITY",           "j.find('ProjCity').text",                        "STRING"  ],
                      ["ADDRESS_POSTALCODE",     "j.find('ProjZip').text",                         "FLOAT"       ],
                      ["ADDRESS_COUNTY",         "j.find('ProjCounty').text",                      "STRING"  ],
                      ["ADDRESS_CANTON",         "canton_dict[j.find('ProjRegId').text]",          "STRING"   ], 
                      ["ADDRESS_COUNTRY",        "j.find('ProjCountryId').text",                   "STRING"   ]]

    building_list  = [["PROJECT_ID",             "l.find('PProProjId').text",                      "STRING"  ],
                      ["BUILDING_TYPE",          "l.find('BuildingType').text",                    "STRING"  ],    
                      ["BUILDING_DEVELOPMENT",   "l.find('DevelopmentType').text",                 "STRING"  ]]

    contact_list   = [["PROJECT_ID",             "n.find('TarProjId').text",                       "STRING"  ],  
                      ["ORG_TYPE",               "n.find('TargetGroupType').text",                 "STRING"  ],   
                      ["ORG_ID",                 "n.find('OrgId').text",                           "STRING"  ], 
                      ["ORG_NAME",               "n.find('OrgName').text",                         "STRING" ], 
                      ["ORG_STREET1",            "n.find('OrgAddress1').text",                     "STRING"  ],                                     
                      ["ORG_STREET2",            "n.find('OrgAddress2').text",                     "STRING"  ], 
                      ["ORG_STREET3",            "n.find('OrgAddress3').text",                     "STRING"  ],                   
                      ["ORG_CITY",               "n.find('OrgCity').text",                         "STRING"  ],
                      ["ORG_COUNTRY",            "n.find('OrgCountryId').text",                    "STRING"   ],
                      ["ORG_POSTALCODE",         "n.find('OrgZip').text",                          "STRING"   ],
                      ["ORG_PB_ADDRESS",         "n.find('OrgPostBoxAddress').text",               "STRING"  ],                                     
                      ["ORG_PB_CITY",            "n.find('OrgPostBoxCity').text",                  "STRING"  ], 
                      ["ORG_PB_POSTALCODE",      "n.find('OrgPostBoxZip').text",                   "STRING"   ],                   
                      ["ORG_PHONE",              "n.find('OrgPhone').text",                        "STRING"  ],
                      ["ORG_EMAIL",              "n.find('OrgEmail').text",                        "STRING"  ],
                      ["ORG_WEB",                "n.find('OrgWeb').text",                          "STRING"  ],
                      ["ORG_EMPLOYEES",          "n.find('Employees').text",                       "STRING"  ],
                      ["PERSON_ID",              "n.find('PerId').text",                           "STRING"  ], 
                      ["PERSON_GENDER",          "n.find('PerGender').text",                       "STRING"   ],                                     
                      ["PERSON_FIRSTNAME",       "n.find('PerFname').text",                        "STRING"  ], 
                      ["PERSON_LASTNAME",        "n.find('PerLname').text",                        "STRING"  ],                   
                      ["PERSON_PHONE",           "n.find('PerPhone').text",                        "STRING"  ],
                      ["PERSON_MOBILE",          "n.find('PerMobile').text",                       "STRING"  ],
                      ["PERSON_EMAIL",           "n.find('PerEmail').text",                        "STRING"  ]]


    #1.5 Feature Exctraction
    print("1.5")

    def df_generator(attr_list):
        df = pd.DataFrame(columns = [i[0] for i in attr_list])
        for i in attr_list:
            df[i[0]] = df[i[0]].astype(object)
        return df

    df_projects = df_generator(project_list) 
    df_buildings = df_generator(building_list) 
    df_contacts = df_generator(contact_list) 
    i_building = i_contact = 0

    for i, j in enumerate(root.find("Projects")):

        #Feature Extraction df_projects
        for k in project_list:
            try: df_projects.at[i, k[0]] = eval(k[1])
            except AttributeError: df_projects.at[i, k[0]] = np.nan

        #Feature Extraction df_buildings    
        for l in j.findall("BuildingType"):
            for k in building_list:
                try: df_buildings.at[i_building, k[0]] = eval(k[1])
                except AttributeError: df_buildings.at[i_building, k[0]] = np.nan
            i_building += 1

        #Feature Extraction df_target    
        for n in j.findall("TargetGroup"):
            for k in contact_list:
                try: df_contacts.at[i_contact, k[0]] = eval(k[1])
                except AttributeError: df_contacts.at[i_contact, k[0]] = np.nan    
            i_contact += 1 

    #1.6 Data Preparation
    #df = df.apply(lambda col: col.apply(lambda x: int(x) if pd.notnull(x) else x), axis=1)
    #fillna(0)
    print("1.6")
    #General Data Preparation
    def data_cleanser(df, attr_list):    
        for i in attr_list: 
            if i[0] in ["PROJECT_INRESEARCH", "DETAIL_SOLAR"]: df[i[0]] = df[i[0]].apply(lambda x: 1 if x == "True" else 0)
            elif i[2]     == "timestamp": df[i[0]] = df[i[0]].apply(lambda x: pd.to_datetime(x, format = "%Y-%m-%dT%H:%M:%S")).fillna(datetime.datetime(1900,1,1))
            elif i[2]     == "FLOAT":     df[i[0]] = df[i[0]].astype(float).fillna(0)
            elif i[2]     == "STRING":    df[i[0]] = df[i[0]].apply(lambda x: x if type(x) != str else x.replace("\r\n", " ").replace("\n", " ")).fillna("")
        return df

    df_projects  = data_cleanser(df_projects,  project_list)
    df_buildings = data_cleanser(df_buildings, building_list)
    df_contacts  = data_cleanser(df_contacts,  contact_list)

    #Cleaning df_projects
    df_projects["PROJECT_FINALUSE"] = df_projects.PROJECT_FINALUSE.map(description_dict['ProjectFinalUse'])
    df_projects["PROJECT_FINALUSE"] = df_projects.PROJECT_FINALUSE.fillna("")
    df_projects["PROJECT_VALUE"] = df_projects.PROJECT_VALUE.apply(lambda x: 0 if x < 0 else x)

    #Cleaning df_contacts
    def phone_number_sanitizer(x):
        if x == None or x=="":                     return None
        else: x = x.replace(" ", "")

        if any(c.isalpha() for c in x):     return None
        elif (x[0] == "0") & (x[1] != "0"): return "+41" + x[1:]
        elif x[:2] == "00":                 return "+" + x[2:]
        return 

    df_contacts["ORG_PHONE"]     = df_contacts.ORG_PHONE.apply(phone_number_sanitizer).fillna("")
    df_contacts["PERSON_PHONE"]  = df_contacts.PERSON_PHONE.apply(phone_number_sanitizer).fillna("")
    df_contacts["PERSON_MOBILE"] = df_contacts.PERSON_MOBILE.apply(phone_number_sanitizer).fillna("")

    #2   Update Database
    #https://stackoverflow.com/questions/51708355/bigquery-standard-sql-not-deleted/51710920
    #https://stackoverflow.com/questions/31652001/big-query-update-or-delete-issue

    if (stage == "DEV") & (df_projects.shape[0]>0):


        dataset_id = 'BINDEXIS'

        load_config_projects = bigquery.LoadJobConfig()
        load_config_projects.schema = [bigquery.SchemaField(i[0], i[2]) for i in project_list]
        load_config_buildings = bigquery.LoadJobConfig()
        load_config_buildings.schema = [bigquery.SchemaField(i[0], i[2]) for i in building_list]
        load_config_contacts = bigquery.LoadJobConfig()
        load_config_contacts.schema = [bigquery.SchemaField(i[0], i[2]) for i in contact_list]

        client_bq = bigquery.Client()
        dataset_ref = client_bq.dataset(dataset_id)
        table_ref_projects = dataset_ref.table('bindexis_bau_projects')
        table_ref_buildings = dataset_ref.table('bindexis_bau_buildings')
        table_ref_contacts = dataset_ref.table('bindexis_bau_contacts')

        #2.1 Delete Records
        print("2.1")
        for i in ["projects", "buildings", "contacts"]:
            if i=="projects":
                tmp_del = list(df_projects["PROJECT_ID"].unique())
            elif i=="buildings":
                tmp_del = list(df_buildings["PROJECT_ID"].unique())
            elif i=="contacts":
                tmp_del = list(df_contacts["PROJECT_ID"].unique())

            client_bq.query("""DELETE FROM {0}.bindexis_bau_{1} where PROJECT_ID IN 
                    (SELECT *
                    FROM UNNEST({2})
                      AS PROJECT_ID)""".format(dataset_id,i,tmp_del)).result()

        #2.2 Insert Updated Records
        print("2.2")    
        client_bq.load_table_from_dataframe(df_projects, table_ref_projects, project=PROJECT, job_config=load_config_projects).result()
        client_bq.load_table_from_dataframe(df_buildings, table_ref_buildings, project=PROJECT, job_config=load_config_buildings).result()
        client_bq.load_table_from_dataframe(df_contacts, table_ref_contacts, project=PROJECT, job_config=load_config_contacts).result()

    #3.2 Speicherung Parameter & Backup bei erfolgreichem Lauf
    print("3.2")
    if stage == "DEV": 
        filename = "{}/Parameter_TimeLastRun.pkl".format(tempfile.gettempdir())
        with open(filename, 'wb') as fp: pickle.dump(time_now, fp)
        blob = bucket_cs.blob(path_data_va+'Parameter_TimeLastRun.pkl')
        blob.upload_from_filename(filename)

    return logging.info('BINDEXIS DATALOAD SUCCESSFUL!')
