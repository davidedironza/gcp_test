
try:
    # change these to try this notebook out
    BUCKET = 'axa-ch-raw-dev-dla'
    PROJECT = 'axa-ch-datalake-analytics-dev'
    REGION = 'eu-west6'

    import os
    os.environ['BUCKET'] = BUCKET
    os.environ['PROJECT'] = PROJECT
    os.environ['REGION'] = REGION

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
    from html import parser
    from random import randint
    from google.cloud import bigquery
    from google.cloud import storage
    import sys


    #0.2 Set Stage
    stage = "DEV" if os.environ['PROJECT']=='axa-ch-datalake-analytics-dev' else "PROD" #sf.platform_is_server("stage")
    print(stage)
    
    #0.3 Pfadinformationen
    path_data_va = "bindexis/data/various/"
    path_data_input = "bindexis/data/input/"

    #0.4 Set timezone
    import time
    os.environ['TZ'] = 'Europe/Zurich'
    time.tzset()

    # 1   Retrieve Bindexis Data
    #1.1 Access & Authentication Setup
    print("1.1")
    username      = "TIppisch"
    time_now      = datetime.datetime.now(pytz.timezone('Europe/Zurich'))

    client_cs = storage.Client()
    bucket_cs = client_cs.get_bucket(BUCKET)
    try:
        filename = "{}/Parameter_TimeLastRun.pkl".format(tempfile.gettempdir())
        blob = bucket_cs.blob(path_data_va+'Parameter_TimeLastRun.pkl')
        blob.download_to_filename(filename)
        with open(filename, 'rb') as fp: time_last_run = pickle.load(fp)
    except:
        time_last_run      = time_now + datetime.timedelta(days=-2)

    print(time_last_run)

    import HTMLParser

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
            </soapenv:Envelope>""".format(username, "19e22172", time_last_run.strftime("%Y-%m-%dT%H:%M:%S"))

    response = requests.post(url     = "https://soap.bindexis.ch/projectsync.asmx",
    #response = requests.post(url     = "https://esg-opco.medc.services.axa-tech.intraxa:8443/AXACH_BindexisWebService",
    #response = requests.post(url = "https://172.20.205.126:8443/AXACH_BindexisWebService",
                             headers = {'content-type': 'text/xml'},
                             #cert    = file,
                             data    = data,
                             verify  = False)

    import xml.sax.saxutils as saxutils
    base_text = saxutils.unescape(response.text.encode("utf-8", errors="ignore"))
    root = ET.fromstring(base_text[base_text.find("<?xml", 1): base_text.find("</GetProjectSyncResult>")])

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

    bool_dict = {'False': False,     'True': True}  

    project_list =   [["PROJECT_ID",             "j.find('ProjId').text",                          "STRING"  ],
                      ["PROJECT_TITLE",          "j.find('ProjTitle').text",                       "STRING"  ],   
                      ["PROJECT_PARCELID",       "j.find('ProjOfficialId').text",                  "STRING"  ],  
                      ["PROJECT_LANGUAGE",       "j.find('ProjLanguageId').text",                  "STRING"  ],
                      ["PROJECT_DESCRIPTION",    "j.find('ProjDesc').text",                        "STRING"  ],
                      ["PROJECT_PLANNINGSTAGE",  "j.find('PlanningStage').text",                   "STRING"  ],
                      ["PROJECT_INRESEARCH",     "bool_dict[j.find('InResearch').text]",           "BOOLEAN" ],                   
                      ["PROJECT_FINALUSE",       "j.find('ProjFinalUseId').text",                  "STRING" ],
                      ["PROJECT_VALUE",          "j.find('ProjValue').text",                       "FLOAT"  ],
                      ["PROJECT_SIZE",           "j.find('ProjSize').text",                        "FLOAT"  ],
                      ["PROJECT_VOLUME",         "j.find('ProjVolume').text",                      "FLOAT"  ],
                      ["PROJECT_APARTMENTS",     "j.find('ProjApartments').text",                  "FLOAT"  ],
                      ["PROJECT_FLOORS",         "j.find('ProjFloors').text",                      "FLOAT"  ],
                      ["PROJECT_BUILDINGS",      "j.find('ProjBuildings').text",                   "FLOAT"  ],                 
                      ["PROJECT_NOTE",           "j.find('ProjNote').text",                        "STRING" ],
                      ["DATE_INSERTION",         "j.find('ProjInsertDate').text",             "TIMESTAMP"    ],
                      ["DATE_UPDATE",            "j.find('ProjLatestUpdate').text",           "TIMESTAMP"    ],
                      ["DATE_PUBLICATION",       "j.find('ProjConfirmDate').text",            "TIMESTAMP"    ],
                      ["DATE_STARTCONSTRUCTION", "j.find('ProjConstStartDate').text",         "TIMESTAMP"    ],
                      ["DATE_PLANNINGAPPROVAL",  "j.find('ProjPlanningApprovalDate').text",   "TIMESTAMP"    ],
                      ["DETAIL_ROOFTYPE",        "j.find('RoofType').text",                        "STRING"  ],
                      ["DETAIL_MATERIALS",       "j.find('ConstructionMaterials').text",           "STRING"  ],
                      ["DETAIL_CLADDING",        "j.find('CladdingType').text",                    "STRING"  ],
                      ["DETAIL_HEATING",         "j.find('HeatingType').text",                     "STRING"  ], 
                      ["DETAIL_SOLAR",           "bool_dict[j.find('ProjSolar').text]",            "BOOLEAN" ], 
                      ["ADDRESS_STREET1",        "j.find('ProjAddress1').text",                    "STRING"  ],                                     
                      ["ADDRESS_STREET2",        "j.find('ProjAddress2').text",                    "STRING"  ], 
                      ["ADDRESS_STREET3",        "j.find('ProjAddress3').text",                    "STRING"  ],                   
                      ["ADDRESS_CITY",           "j.find('ProjCity').text",                        "STRING"  ],
                      ["ADDRESS_POSTALCODE",     "j.find('ProjZip').text",                         "INT"     ],
                      ["ADDRESS_COUNTY",         "j.find('ProjCounty').text",                      "STRING"  ],
                      ["ADDRESS_CANTON",         "j.find('ProjRegId').text",                      "STRING"   ], 
                      ["ADDRESS_COUNTRY",        "j.find('ProjCountryId').text",                   "STRING"  ]]

    building_list  = [["PROJECT_ID",             "l.find('PProProjId').text",                      "STRING"  ],
                      ["BUILDING_TYPE",          "l.find('BuildingType').text",                    "STRING"  ],    
                      ["BUILDING_DEVELOPMENT",   "l.find('DevelopmentType').text",                 "STRING"  ]]

    contact_list   = [["PROJECT_ID",             "n.find('TarProjId').text",                       "STRING"  ],  
                      ["ORG_TYPE",               "n.find('TargetGroupType').text",                 "STRING"  ],   
                      ["ORG_ID",                 "n.find('OrgId').text",                           "STRING"  ], 
                      ["ORG_NAME",               "n.find('OrgName').text",                         "STRING"  ], 
                      ["ORG_STREET1",            "n.find('OrgAddress1').text",                     "STRING"  ],                                     
                      ["ORG_STREET2",            "n.find('OrgAddress2').text",                     "STRING"  ], 
                      ["ORG_STREET3",            "n.find('OrgAddress3').text",                     "STRING"  ],                   
                      ["ORG_CITY",               "n.find('OrgCity').text",                         "STRING"  ],
                      ["ORG_COUNTRY",            "n.find('OrgCountryId').text",                    "STRING"  ],
                      ["ORG_POSTALCODE",         "n.find('OrgZip').text",                          "STRING"  ],
                      ["ORG_PB_ADDRESS",         "n.find('OrgPostBoxAddress').text",               "STRING"  ],                                     
                      ["ORG_PB_CITY",            "n.find('OrgPostBoxCity').text",                  "STRING"  ], 
                      ["ORG_PB_POSTALCODE",      "n.find('OrgPostBoxZip').text",                   "STRING"  ],                   
                      ["ORG_PHONE",              "n.find('OrgPhone').text",                        "STRING"  ],
                      ["ORG_EMAIL",              "n.find('OrgEmail').text",                        "STRING"  ],
                      ["ORG_WEB",                "n.find('OrgWeb').text",                          "STRING"  ],
                      ["ORG_EMPLOYEES",          "n.find('Employees').text",                       "STRING"  ],
                      ["PERSON_ID",              "n.find('PerId').text",                           "STRING"  ], 
                      ["PERSON_GENDER",          "n.find('PerGender').text",                       "STRING"  ],                                     
                      ["PERSON_FIRSTNAME",       "n.find('PerFname').text",                        "STRING"  ], 
                      ["PERSON_LASTNAME",        "n.find('PerLname').text",                        "STRING"  ],                   
                      ["PERSON_PHONE",           "n.find('PerPhone').text",                        "STRING"  ],
                      ["PERSON_MOBILE",          "n.find('PerMobile').text",                       "STRING"  ],
                      ["PERSON_EMAIL",           "n.find('PerEmail').text",                        "STRING"  ]]

    #1.5 Feature Exctraction
    print("1.5")

    def df_generator(attr_list):
        df = {i[0] for i in attr_list}
        return df

    df_projects = []
    df_buildings = []
    df_contacts = []

    for i, j in enumerate(root.find("Projects")):
        #print("loop1",i)
        #Feature Extraction df_projects
        df_project = {} 
        for k in project_list:
            #print("loop2",i)
            #try: df_project[k[0]] = eval(k[1])
            try: 
                if k[2]=="FLOAT" and eval(k[1]) is not None: df_project[k[0]] = float(eval(k[1]))
                elif k[2]=="INT" and eval(k[1]) is not None: df_project[k[0]] = int(eval(k[1]))
                else: df_project[k[0]] = eval(k[1])
            except AttributeError: df_project[k[0]] = None
        df_projects.append(df_project)

        #Feature Extraction df_buildings    
        df_building = {} 
        for l in j.findall("BuildingType"):
            for k in building_list:
                try: 
                    if k[2]=="FLOAT" and eval(k[1]) is not None: df_building[k[0]] = float(eval(k[1]))
                    elif k[2]=="INT" and eval(k[1]) is not None: df_building[k[0]] = int(eval(k[1]))
                    else: df_building[k[0]] = eval(k[1])
                except AttributeError: df_building[k[0]] = None
        df_buildings.append(df_building)

        #Feature Extraction df_target   
        df_contact = {}  
        for n in j.findall("TargetGroup"):
            for k in contact_list:
                try: 
                    if k[2]=="FLOAT" and eval(k[1]) is not None: df_contact[k[0]] = float(eval(k[1]))
                    elif k[2]=="INT" and eval(k[1]) is not None: df_contact[k[0]] = int(eval(k[1]))
                    else: df_contact[k[0]] = eval(k[1])
                except AttributeError: df_contact[k[0]] = None 
        df_contacts.append(df_contact)



    #1.6 Save Data as avro
    print("1.6")

    import uuid
    import avro.schema
    import json
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter

    def save_avro_output(file_name, records, list_name):
        """
        Given a list of records, saves to file in Avro format.
        :param file_name: name of file to write Avro data
        :param records: list of dicts, containing the actual records to be saved
        :param list_name: list of list, containing the column names and types
        :return: 
        """
        avro_schema = avro.schema.parse(json.dumps({
            "namespace": file_name,
            "type": "record",
            "name": file_name,
            "fields": [{"name": i[0], "type": [i[2].lower(),"null"]} for i in list_name if i[2].lower()!="timestamp"] +
                      [{"name": i[0], "type": [{"type": "string", "logicalType": "timestamp-millis"}, "null"]} for i in list_name if i[2].lower()=="timestamp"]
        }))

        #os.remove(file_name)
        writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), avro_schema)

        for record in records:
            writer.append(record)

        writer.close()

    save_avro_output('{}/df_projects.avro'.format(tempfile.gettempdir()), df_projects, project_list)
    save_avro_output('{}/df_buildings.avro'.format(tempfile.gettempdir()), df_buildings, building_list)
    save_avro_output('{}/df_contacts.avro'.format(tempfile.gettempdir()), df_contacts, contact_list)

    blob = bucket_cs.blob(path_data_va+'df_projects.avro')
    blob.upload_from_filename('{}/df_projects.avro'.format(tempfile.gettempdir()))
    blob = bucket_cs.blob(path_data_va+'df_buildings.avro')
    blob.upload_from_filename('{}/df_buildings.avro'.format(tempfile.gettempdir()))
    blob = bucket_cs.blob(path_data_va+'df_contacts.avro')
    blob.upload_from_filename('{}/df_contacts.avro'.format(tempfile.gettempdir()))


    #2.1   Update Database
    print("2.1")
    # https://beam.apache.org/documentation/io/built-in/google-bigquery/
    # https://beam.apache.org/documentation/programming-guide/#pcollections
    import apache_beam as beam
    import datetime
    import shutil, os, subprocess
    from apache_beam.io.gcp.internal.clients import bigquery

    DATASET = 'BINDEXIS'

    def preprocess(in_test_mode, file_name, project_id, bucket_id, dataset_id, table_id, list_name):
        import shutil, os, subprocess
        job_name = 'bindexis-dataload' + '-' + file_name + '-' + datetime.datetime.now().strftime('%y%m%d-%H%M%S')

        table_spec = bigquery.TableReference(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id)

        table_schema = {
            "fields": [{"name": i[0] , "type": i[2].replace('INT','INTEGER')} for i in list_name]
        }

        if in_test_mode:
            print('Launching local job ... hang on')
            OUTPUT_DIR = './preproc'
            shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
            os.makedirs(OUTPUT_DIR)
        else:
            print('Launching Dataflow job {} ... hang on'.format(job_name))
            OUTPUT_DIR = 'gs://{0}/bindexis/data/various/'.format(bucket_id)
            try:
                subprocess.check_call('gsutil -m rm -r {}'.format(OUTPUT_DIR).split())
            except:
                pass

        options = {
            'staging_location': os.path.join(OUTPUT_DIR, 'tmp', 'staging'),
            'temp_location': os.path.join(OUTPUT_DIR, 'tmp'),
            'job_name': job_name,
            'project': project_id,
            'region': REGION,
            'teardown_policy': 'TEARDOWN_ALWAYS',
            'no_save_main_session': True
        }
        opts = beam.pipeline.PipelineOptions(flags = [], **options)

        if in_test_mode:
            RUNNER = 'DirectRunner'
        else:
            RUNNER = 'DataflowRunner'
        p = beam.Pipeline(RUNNER, options = opts)

        import json

        # Transformations

        #General Data Preparation        
        class data_cleanser(beam.DoFn):
            def process(self, data_item):    
                data_item = json.dumps(data_item, separators=(',', ': '))
                data_item = json.loads(data_item)

                for i in list_name: 
                    #if i[0] in ["PROJECT_INRESEARCH", "DETAIL_SOLAR"]: df[i[0]] = df[i[0]].apply(lambda x: 1 if x == "True" else 0)
                    #elif i[2]     == "TIMESTAMP": df[i[0]] = df[i[0]].apply(lambda x: pd.to_datetime(x, format = "%Y-%m-%dT%H:%M:%S")).fillna(datetime.datetime(1900,1,1))
                    #elif i[2]     == "FLOAT":     df[i[0]] = df[i[0]].astype(float).fillna(0)
                    if (i[2]     == "STRING") & (data_item[i[0]]!=None):    data_item[i[0]] = data_item[i[0]].replace("\r\n", " ").replace("\n", " ")

                #Cleaning df_projects
                if table_id=='df_projects':
                    data_item = json.dumps(data_item, separators=(',', ': '))
                    data_item = json.loads(data_item)
                    data_item['ADDRESS_CANTON'] = unicode(canton_dict[data_item['ADDRESS_CANTON']], "utf-8")
                    data_item['PROJECT_LANGUAGE'] = data_item['PROJECT_LANGUAGE'][0:2].upper()
                    data_item["PROJECT_FINALUSE"] = unicode(description_dict['ProjectFinalUse'][data_item["PROJECT_FINALUSE"]], "utf-8") if data_item["PROJECT_FINALUSE"]!=None else data_item["PROJECT_FINALUSE"]
                    data_item["PROJECT_VALUE"] = 0 if data_item["PROJECT_VALUE"] < 0 else data_item["PROJECT_VALUE"]

                #Cleaning df_contacts
                if table_id=='df_contacts':
                    def phone_number_sanitizer(x):
                        if x == None or x=="":                     return None
                        else: x = x.replace(" ", "")

                        if any(c.isalpha() for c in x):     return None
                        elif (x[0] == "0") & (x[1] != "0"): return "+41" + x[1:]
                        elif x[:2] == "00":                 return "+" + x[2:]
                        return 

                    data_item["ORG_PHONE"]     = phone_number_sanitizer(data_item["ORG_PHONE"])
                    data_item["PERSON_PHONE"]  = phone_number_sanitizer(data_item["PERSON_PHONE"])
                    data_item["PERSON_MOBILE"] = phone_number_sanitizer(data_item["PERSON_MOBILE"])

                yield data_item


        class Printer(beam.DoFn):
            def process(self,data_item):
                print (data_item)


    #    #2.1 Delete Records
    #    if i=="projects":
    #        tmp_del = list(df_projects["PROJECT_ID"].unique())
    #    elif i=="buildings":
    #        tmp_del = list(df_buildings["PROJECT_ID"].unique())
    #    elif i=="contacts":
    #        tmp_del = list(df_contacts["PROJECT_ID"].unique())
    #
    #    client_bq.query("""DELETE FROM {0}.bindexis_bau_{1} where PROJECT_ID IN 
    #            (SELECT *
    #           FROM UNNEST({2})
    #              AS PROJECT_ID)""".format(dataset_id,i,tmp_del)).result()


        # Write the file
        (p
            | 'ReadAvroFromGCS' >> beam.io.avroio.ReadFromAvro('gs://{0}/{1}{2}'.format(BUCKET,path_data_va,file_name))
            | 'data_cleanser' >> beam.ParDo(data_cleanser(list_name))
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )

        # Run the pipeline
        job = p.run()
        if in_test_mode:
            job.wait_until_finish()
            print("Done!")

#    preprocess(in_test_mode=True, file_name='df_projects.avro', 
#               project_id=PROJECT, bucket_id=BUCKET, 
#               dataset_id=DATASET, table_id='bindexis_bau_projects2', list_name=project_list)

#    preprocess(in_test_mode=True, file_name='df_buildings.avro', 
#               project_id=PROJECT, bucket_id=BUCKET, 
#               dataset_id=DATASET, table_id='bindexis_bau_buildings2', list_name=building_list)

#    preprocess(in_test_mode=True, file_name='df_contacts.avro', 
#               project_id=PROJECT, bucket_id=BUCKET, 
#               dataset_id=DATASET, table_id='bindexis_bau_contacts2', list_name=contact_list)

except Exception:
    #3   Exception Handling, Backup, Reporting
    #3.1 eMail-Benachrichtigung Exception
    print("3.1")
    time_now_ex      = datetime.datetime.now(pytz.timezone('Europe/Zurich'))
    email_to = "davide.dironza@axa.ch; gerhard.pachl@axa.ch; "   
    email_subject = u"CC D&A Scheduling: Aufbereitung Bindexis-Bauausschreibungen"
    email_body = u"""Lieber Tobias, 
    die Aufbereitung der Bindexis-Bauausschreibungen verursachte einen Fehler. 
    Datum: {0}
    Fehlermeldung: 

    {1}

    This mail is powered by Python, the Programming Language of Leading Data Scientists.""".format(time_now_ex.strftime('%d.%m.%Y'), traceback.format_exc())
    
    #aux.send_email(subject = email_subject, body = email_body, to = email_to, use_amg = True)

    print(traceback.format_exc())

else:     
    #3.2 Speicherung Parameter & Backup bei erfolgreichem Lauf
    print("3.2")
    if stage == "DEV":
    #if stage == "PROD": 
        filename = "{}/Parameter_TimeLastRun.pkl".format(tempfile.gettempdir())
        with open(filename, 'wb') as fp: pickle.dump(time_now, fp)
        blob = bucket_cs.blob(path_data_va+'Parameter_TimeLastRun.pkl')
        blob.upload_from_filename(filename)

    sys.exit(0)
