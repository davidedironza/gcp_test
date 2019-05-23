# -*- coding: utf-8 -*-
from uuid     import uuid4 
from lxml     import etree
from html     import escape
from pandas   import notnull
from getpass  import getuser
from datetime import datetime, timezone
from requests import get, post
from re       import match, DOTALL

from requests.packages.urllib3 import disable_warnings

#import CCDA_Standard_Functions as sf
import traceback

import xml.etree.cElementTree as ET

# first get certificate file  - 20190515*gep new for test purposes
from google.cloud import storage
import tempfile


#%% General Functions 
def soa_createsoapheader(appsystem = "APP-1825", namespace = "", username = getuser()):
    """Create a standardized XML header (str) for SOA service calls. 
    The default attributes of the function call should be suitable for most API calls. 

    Owner: Tobias Ippisch

    Keyword Arguments:
    appsystem << str (optional) >>
                 Application-ID of calling system (CCDA default: APP-1825); cf. MEGA for more information
    namespace << str (optional) >>
                 Namespace extension for the definition of attribute groups (e.g., mar:)
    username  << str (optional) >>
                 User-Id of calling system (default: currently logged in user). 
                 Supports user concepts like C-, H-, V-, and E-user 

    Return Value:
    << str >>
    """

    now_utc = datetime.utcnow()

    ns_dict = {'':      ['',                                                                   "%Y-%m-%dT%H:%M+02:00"],
               'aems:': ['xmlns:aems="http://aems.corp.intraxa/eip/2012/03/schemas/envelope"', "%Y-%m-%dT%H:%M:%S+02:00"]}
    
    assert(namespace in ns_dict.keys()), "Namespace not defined"

    header_string = """<soapenv:Header>
                           <contextHeader {8}>
                               {0}
                               <addressing>{1} {2} {3}</addressing>
                               <requesters totalCount="1"><requester order="0">{4} {5} {6}</requester></requesters>
                           </contextHeader>
                           <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                               <wsse:UsernameToken>{7}</wsse:UsernameToken>
                           </wsse:Security>
                       </soapenv:Header>"""
                       
    header_list = [["auditTimestamp",    namespace, now_utc.strftime(ns_dict[namespace][1])],
                   ["messageID",         namespace, str(uuid4())],
                   ["requestMessageID",  namespace, str(uuid4())],
                   ["conversationID",    namespace, str(uuid4())],
                   ["opCo",              namespace, "CH"],
                   ["applicationSystem", namespace, appsystem],  
                   ["creationTimestamp", namespace, now_utc.strftime(ns_dict[namespace][1])],                     
                   ["Username",          "wsse:",   username]]

                                           
    return header_string.format(*[soa_xmlattributer(i[0], i[1], i[2]) for i in header_list], ns_dict[namespace][0])                      

def soa_validateschema(xml, path_xsd):
    """Validates an XML string against an XML schema (.xsd file). Returns a boolean object indicating whether the 
    string passes the validation test. 

    Owner: Tobias Ippisch

    Keyword Arguments:
    xml      << str >>
                String containing an XML used for a SOAP service call.
    path_xsd << str >>
                File path to the .xsd file containing the XML schema. Note that the XML schema string cannot be passed directly. 

    Return Value:
    << bool >>
       Boolean value indicating a successful validation. False indicates a failed schema validation, True a successful one. 
    """
    
    xml_schema = etree.XMLSchema(etree.parse(path_xsd))
    xml        = etree.fromstring(xml)
    
    return xml_schema.validate(xml)

def soa_apicall(method, url, data, cert, raise_exception = False):
    """Performs a standadized SOA service call. Supports both GET and POST method.  

    Owner: Tobias Ippisch

    Keyword Arguments:
    method          << str >>
                       Method of HTTP service call; supported methods are "get" and "post" 
    url             << str >>
                       URL to which service call is sent
    data            << str >>
                       Payload of request; can contain SOAP/XML request string (incl. SOA header)
                       or REST/JSON request string; request string must comply with interfacae specification
    cert            << str >>
                       Absolute or relative path to certificate file (.pem)
    raise_exception << bool (optional) >>
                       Indicates whether service calls not resulting in a HTTP status 200 
                       should raise an exception (ValueError)
    Return Value:
    << requests.models.Response >>
       Response object of the service call. Has attributes like .status_code or .text
    """    
    
    disable_warnings()
    if   method == "get":  response = get( url = url, data = data, cert = cert, verify = False)
    elif method == "post": response = post(url = url, data = data, cert = cert, verify = False)
    else: raise ValueError("Inappropriate HTTP method")
    
    #Error-Handling
    if (raise_exception == True) & (response.status_code != 200):
        raise ValueError("HTTP Response {0} ({1})".format(response.status_code, response.reason))
    
    return response

def soa_xmlattributer(attribute, namespace, value):
    """Auxiliary function to create standardized XML-strings of the form
    <extension:attribute>value</extension:attribute> to use in SOAP service calls. 

    Owner: Tobias Ippisch

    Keyword Arguments:
    attribute  << str >>
                  Name of xml tag (e.g., nname)
    namespace  << str >>
                  Namespace extension for the definition of attribute groups (e.g., mar:)
                  Cf. SOA interface descriptions (.wsdl) for usage examples of attribute extensions
    value      << str >>
                  Value of XML attribute (e.g., Meier)

    Return Value:
    << str >>"""    
      
    if (value != ""): return "<{1}{0}>{2}</{1}{0}>".format(attribute, namespace, escape(value)) 
    else:             return "" 

def soa_dictmapper(row, mapping):
    """Auxiliary function for creating automated mappings between SOAP service call parameters
    and pd.DataFrame columns. To be used in various wrapper function of SOAP service calls. 

    Owner: Tobias Ippisch

    Keyword Arguments:
    row     << pd.Series >>
               Series containing all the values of a single DataFrame row. 
               Best retrieved by: df.apply(lambda row: ...)
    mapping << dict or list>>
               Mapping for potential function input parameter dictionaries. 
               Keys include the possible parameters of the SOA service (str), 
               the values the mapped column names (str) of the pd.DataFrame.
               Please note that no fix values can be provided with this parameter; 
               the dict values will always try to reference a pd.DataFrame column.

    Return Value:
    << dict >>"""     
    
    if type(mapping) == dict:
        aux_dict = {}
        for i in mapping.keys(): 
            aux_dict[i] = row[mapping[i]]  
        
    elif type(mapping) == list:
        aux_dict = []
        for i in mapping:
            aux_dict.append([i[0], row[i[1]]])
              
    return aux_dict


#%% Interface-specific Functions 
def soa_marketinginteraktiondatenpush_1(interaction_dict, contact_dict, nvp_dict = {},
                                        header = soa_createsoapheader(), stage = "DEV"): 
    """Initiates a SOA service call to the API "MarketingInteraktionDatenPush_1". With this service, interactions and contacts can be 
    provided to Hybris-Marketing (yMKT) for use in marketing campaigns. See the SOA documentation for more details on this webservice: 
    http://soa.wgrintra.net/ch/architecture/services/template/inhalt.htmls?/ch/architecture/services/MarketingInteraktionDatenPush_1/    

    Owner: Tobias Ippisch

    Keyword Arguments:
    interaction_dict << dict >>
                        Dictionary of name-value-pairs describing the interaction. The following variables are supported:
                        - iaktn_art_cdymkt    (str)
                        - komkn_medium_cdymkt (str)
                        - ref_bo_klsfkn_cdu   (str, optional) Note that only one reference business object is supported (contrary to the SOA API)
                        - ref_bo_id           (str, optional) Note that only one reference business object is supported (contrary to the SOA API)
                        - v_prod_klsfkn_cdcrm (str, optional) Note that only one product is supported (contrary to the SOA API)
                        - v_prod_cd_wert      (str, optional) Note that only one product is supported (contrary to the SOA API)
                        - ntz_txt_lang        (str, optional)
                        - url_v1              (str, optional)  
                       
    contact_dict     << dict >>
                        Dictionary of name-value-pairs describing the contact. The contact can be a new or existing partner.
                        The following variables are supported:
                        - iaktn_kntkt_ref_id             (str)
                        - iaktn_kntkt_ref_typ_cdymkt     (str)                                                
                        - fct_iaktn_kntkt_ref_id",       (str, optional) Note that only one facette is supported (contrary to the SOA API)
                        - fct_iaktn_kntkt_ref_typ_cdymkt (str, optional) Note that only one facette is supported (contrary to the SOA API)
                        - gpart_typ_cdgpd                (str, optional) Cf. approved element    
                        - name_nnp_zeile1                (str, optional)            
                        - anred_ashrft_cdgpd             (str, optional) Cf. approved element  
                        - vname                          (str, optional)
                        - nname                          (str, optional)
                        - geb_dat                        (str, optional) Format yyyy-mm-dd            
                        - sex_cdu                        (int, optional) Cf. approved element
                        - natt_cdi                       (str, optional) 2 character ISO format 
                        - spra_cdi_korrz                 (str, optional) 2 character ISO format
                        - zivst_cdu                      (int, optional) Cf. approved element                    
                        - email_adr                      (str, optional)
                        - tel_nr_kompl                   (str, optional)
                        - tel_nr_kompl_mobil             (str, optional)
                        - str_name                       (str, optional)
                        - haus_nr_kompl                  (str, optional)         
                        - plz                            (str, optional)
                        - ort_name                       (str, optional) 
                        - land_cdi                       (str, optional) 2 character ISO format
                                                                                
    nvp_dict         << dict, list (optional) >>
                        Dictionary of name-value-pairs for submitting additional information through the service call 
                        (e.g. distribution channel or campagin). Contrary to interaction_dict and contact_dict, nvp_dict 
                        also supports float and int as its dict values. 
                        Alternatively, the key-value-pairs can be provided as list of lists or list of tuples.
    header           << str (optional) >>
                        Header string for the SOA call. The default function creates an automated header 
                        string suitable for most service calls
    stage            << str (optional) >>
                        Defines the staging area on which to performm the call. Possible values include
                        "DEV", "ACC", and "PROD". 
    
    Return Value:
    << str >>
       String containing either an exception or the HTTP status code of the API request. 
       "OK" indicates that the API call was done successfully."""    
    
    
    # first get certificate file  - 20190515*gep new for test purposes
    
    def get_cert_file(bucket_in,path_data_pem_in,cert_file_in):
        client_cs = storage.Client()
        bucket_cs = client_cs.get_bucket(bucket_in)
    
        try:
            file = "{0}/{1}".format(tempfile.gettempdir(),cert_file_in)
            blob = bucket_cs.blob(path_data_pem_in + cert_file_in)
            blob.download_to_filename(file)
        except:
            print("except")
            
        return file
    
    #if stage == "ACC": # 20190523*gep normal get different cert files for different stages
    bucket        = "axa-ch-datalake-analytics-dev"
    path_data_pem = "certificates/acc/"
    cert_file     = "sas_server_keystore.pem"
    
    filename = get_cert_file(bucket,path_data_pem,cert_file)
    print(filename)
    # end first get certificate file  - 20190515*gep new for test purposes 
    
    try: 
        #A) Parameter Setting & Function Definition     
        stage_dict = {"DEV":  ["https://soadev.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
                              filename],
#                      "ACC":  ["https://soaacc.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
#                               filename],
# Achtung - jetzt noch IP-Adresse direkt aufrufen, später sollte das wieder durch https://soaacc.ch.winterthur.com:8443/ ... ersetzt werden                      
                      "ACC":  ["https://10.152.124.139:8443/MarketingInteraktionDatenPush_1", 
                               filename],                      
                      "PROD": ["https://soaprod.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
                               filename]}
 
        #stage_dict = {"DEV":  ["https://soadev.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       r"{}\CCDA\09_Betrieb\00_Files\test-client-soapui.pem".format(sf.platform_is_server("drive"))],
        #              "ACC":  ["https://soaacc.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       filename],
        #              "PROD": ["https://soaprod.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       r"E:\certs\client\sas_server_keystore.pem"]}        
              
        def aux_asserter(input_list, input_dict): 
            for i in input_list:
                x = input_dict.get(i[0], None)
                if notnull(x): assert(match(i[2], x, DOTALL)), "{} does not match format {}".format(i[0], i[2])
                
        def aux_generator(input_list, input_dict):
            output_string = ""
            for i in input_list: 
                x = input_dict.get(i[0], None)
                if (notnull(x)) & (x != "") :
                    if   i[1] == "str":        output_string += soa_xmlattributer(i[0], "mar:", input_dict[i[0]])
                    elif i[1] == "url":        output_string += "<mar:iaktn_ref_url>" + soa_xmlattributer(i[0], "mar:", input_dict[i[0]]) + "</mar:iaktn_ref_url>" 
                    elif i[1] == "fct_start":  output_string += "<mar:iaktn_kontakt_facetten>"  + soa_xmlattributer(i[0][4:], "mar:", input_dict[i[0]])
                    elif i[1] == "fct_end":    output_string += soa_xmlattributer(i[0][4:], "mar:", input_dict[i[0]]) + "</mar:iaktn_kontakt_facetten>" 
                    elif i[1] == "prod_start": output_string += "<mar:iaktn_produkt>"  + soa_xmlattributer(i[0], "mar:", input_dict[i[0]])
                    elif i[1] == "prod_end":   output_string += soa_xmlattributer(i[0], "mar:", input_dict[i[0]]) + "</mar:iaktn_produkt>" 
                    elif i[1] == "bo_start":   output_string += "<mar:iaktn_ref_bo>"  + soa_xmlattributer(i[0], "mar:", input_dict[i[0]])
                    elif i[1] == "bo_end":     output_string += soa_xmlattributer(i[0], "mar:", input_dict[i[0]]) + "</mar:iaktn_ref_bo>" 
               
            return output_string
    
        #B) Handling Interaction-Head
        #                    Variable Name          Type          Regex               
        interaction_list = [["iaktn_art_cdymkt",    "str",        "^[0-9]{1,2}$"],
                            ["komkn_medium_cdymkt", "str",        "^[0-9]{1,2}$"], 
                            ["v_prod_klsfkn_cdcrm", "prod_start", "^.{0,30}$"],
                            ["v_prod_cd_wert",      "prod_end",   "^.{0,10}$"],
                            ["ref_bo_klsfkn_cdu",   "bo_start",   "^.{0,30}$"],
                            ["ref_bo_id",           "bo_end",     "^.{0,64}$"],
                            ["url_v1",              "url",        "^.{0,1000}$"],  
                            ["ntz_txt_lang",        "str",        "^.{0,5000}$"]]
        
                                   
        #   Assertions    
        for i in interaction_list[:2]: assert(i[0] in interaction_dict.keys()), "{} required in interaction_dict".format(i)       
        aux_asserter(interaction_list, interaction_dict)
        
        #   String Generation  
        offset = datetime.now(timezone.utc).astimezone().strftime('%z')
        offset= offset[:3]+":"+offset[3:]
        time_stamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+offset       
        interaction_string =  aux_generator(interaction_list[:2], interaction_dict) 
        interaction_string += soa_xmlattributer("iaktn_beg_zpkt", "mar:", time_stamp)
    
        #C) Handling Contact-Data        
        #                Variable Name                     Type         Regex           
        contact_list = [["iaktn_kntkt_ref_id",             "str",       "^[0-9a-zA-Z\-]{1,255}$"],
                        ["iaktn_kntkt_ref_typ_cdymkt",     "str",       "^[0-9]{1,2}$"],
                        ["fct_iaktn_kntkt_ref_id",         "fct_start", "^[0-9a-zA-Z\-]{0,255}$"],
                        ["fct_iaktn_kntkt_ref_typ_cdymkt", "fct_end",   "^[0-9]{0,2}$"],
                        ["gpart_typ_cdgpd",                "str",       "^[1-2]{0,1}$"],            
                        ["name_nnp_zeile1",                "str",       "^.{0,35}$"],                      
                        ["anred_ashrft_cdgpd",             "str",       "^[1-2]{0,1}$"], 
                        ["vname",                          "str",       "^.{0,40}$"],     
                        ["nname",                          "str",       "^.{0,40}$"],     
                        ["geb_dat",                        "str",       "^(\d{4}[-](0?[1-9]|1[012])[-](0?[1-9]|[12][0-9]|3[01]))|(.{0})$"],                
                        ["sex_cdu",                        "str",       "^[0-2]{0,1}$"], 
                        ["natt_cdi",                       "str",       "^([A-Z]{0}|[A-Z]{2})$"],
                        ["spra_cdi_korrz",                 "str",       "^([A-Z]{0}|[A-Z]{2})$"],
                        ["zivst_cdu",                      "str",       "^[1-9]{0,1}$"],                     
                        ["email_adr",                      "str",       "^.{0,241}$"], 
                        ["tel_nr_kompl",                   "str",       "^[0-9\+ ]{0,30}$"],
                        ["tel_nr_kompl_mobil",             "str",       "^[0-9\+ ]{0,30}$"],
                        ["str_name",                       "str",       "^.{0,60}$"], 
                        ["haus_nr_kompl",                  "str",       "^.{0,10}$"],            
                        ["plz",                            "str",       "^.{0,10}$"], 
                        ["ort_name",                       "str",       "^.{0,40}$"],  
                        ["land_cdi",                       "str",       "^([A-Z]{0}|[A-Z]{2})$"]] 
    
        #   Assertions
        for i in contact_list[:2]: assert(i[0] in contact_dict.keys()), "{} required in contact_dict".format(i)        
        aux_asserter(contact_list, contact_dict)
                
        #   String Generation
        contact_string = aux_generator(contact_list, contact_dict)
             
        #D) Handling Interaction Content-Data
        #   String Generation
        content_string =  aux_generator(interaction_list[2:], interaction_dict)  
        
        #E) Handling NVP-Elements
        type_dict = {str: "txt", int: "intgr", float: "decml"}
     
        #   Assertions
        if   type(nvp_dict) == dict: nvp_dict = list(nvp_dict.items())
        elif type(nvp_dict) == list: nvp_dict = nvp_dict
        
        for i in nvp_dict:
            assert(type(i[0]) == str), "nvp_dict key not of type string"
            assert(len(i[0]) <= 32), "nvp_dict key name exceeds 32 characters"
                
            if type(i[1]) == str:
                assert(len(i[1]) <= 255), "nvp_dict value name exceeds 255 characters"
        
        #   String Generation  
        nvp_string = ""
        
        for i in nvp_dict:
            if i[1] is not None:
                nvp_string += "<mar:ergzg>" + soa_xmlattributer("nvp_name", "mar:", i[0]) + \
                              soa_xmlattributer("nvp_value_{}".format(type_dict[type(i[1])]), "mar:", str(i[1])) + "</mar:ergzg>"
                        
        #F) Schema Validation 
        data = """<mar:pass_interaktion_kontakt xmlns:mar="http://soa.wgrintra.net/ch/architecture/MarketingInteraktionDatenPush_1">
                       <mar:lpsoi074>
                           <mar:interaktion>
                               {0}
                               <mar:iaktn_kontakt>{1}</mar:iaktn_kontakt>   
                               {2} 
                               {3}
                           </mar:interaktion>       
                       </mar:lpsoi074>
                  </mar:pass_interaktion_kontakt>""".format(interaction_string, contact_string, content_string, nvp_string)
        
        # xsd_file = "{}/CCDA/09_Betrieb/00_Files/MarketingInteraktionDatenPush_1.xsd".format(sf.platform_is_server("drive"))         
            # first get certificate file  - 20190515*gep new for test purposes
    
        def get_xsd_file(bucket_in,path_data_pem_in,cert_file_in):
            client_cs = storage.Client()
            bucket_cs = client_cs.get_bucket(bucket_in)
    
            try:
                file = "{0}/{1}".format(tempfile.gettempdir(),cert_file_in)
                blob = bucket_cs.blob(path_data_pem_in + cert_file_in)
                blob.download_to_filename(file)
            except:
                print("except")
            
            return file
    
        if stage == "ACC":
            bucket        = "axa-ch-raw-dev-dla"
            path_data_pem = "bindexis/data/various/"
            xsd_file_get     = "MarketingInteraktionDatenPush_1.xsd"
    
        xsd_file = get_xsd_file(bucket,path_data_pem,xsd_file_get)
        print("xsd ", xsd_file)
        
        if soa_validateschema(data, xsd_file) == False:
            raise AssertionError("XML schema validation failed for: {}".format(data))
            
        #G) XML-String Generation 
        data = """<?xml version="1.0" encoding="UTF-8"?>
                  <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mar="http://soa.wgrintra.net/ch/architecture/MarketingInteraktionDatenPush_1">
                      {0}
                  <soapenv:Body>{1}</soapenv:Body>
                  </soapenv:Envelope>""".format(header, data).encode("utf-8")
    
        #H) POST-Request Execution
        response = soa_apicall(method = "post", url = stage_dict[stage][0], data = data, cert = stage_dict[stage][1])
        status = response.reason
    except Exception:
        status = "Error: {}".format(traceback.format_exc())
    
    print("status: ", status) 
    return status
    
def soa_wrapper_marketinginteraktiondatenpush_1(row, interaction_mapping, contact_mapping, 
                                                nvp_mapping = {}, stage = "DEV"):
    """Wrapper for soa_marketinginteractiondatenpush_1 allowing to perform multiple SOA service 
    calls from a pd.DataFrame. The mapping  parameters map the required input parameters of the SOAP service 
    to DataFrame columns.

    Owner: Tobias Ippisch

    Keyword Arguments:
    row                 << pd.Series >>
                           Series containing all the values of a single DataFrame row. 
                           Best retrieved by: df.apply(lambda row: ...)
                           
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
                            
    stage               << str (optional) >>
                           Defines the staging area on which to performm the call. Possible values include
                           "DEV", "ACC", and "PROD". 
    
    Return Value:
    << str >>
       String containing either an exception or the HTTP status code of the API request. 
       "OK" indicates that the API call was done successfully.
       
    Dictionary Templates:
    interaction_mapping = {"iaktn_art_cdymkt":    "",
                           "komkn_medium_cdymkt": "",
                           "ref_bo_klsfkn_cdu":   "",
                           "ref_bo_id":           "",
                           "ntz_txt_lang":        ""}
        
    contact_mapping = {"iaktn_kntkt_ref_id":             "",
                       "iaktn_kntkt_ref_typ_cdymkt":     "",        
                       "fct_iaktn_kntkt_ref_id":         "",
                       "fct_iaktn_kntkt_ref_typ_cdymkt": "",
                       "gpart_typ_cdgpd":                "",
                       "name_nnp_zeile1":                "",
                       "anred_ashrft_cdgpd":             "",
                       "vname":                          "",
                       "nname":                          "",
                       "geb_dat":                        "",
                       "sex_cdu":                        "",
                       "natt_cdi":                       "",
                       "spra_cdi_korrz":                 "",
                       "zivst_cdu":                      "",
                       "email_adr":                      "",
                       "tel_nr_kompl":                   "",
                       "tel_nr_kompl_mobil":             "",
                       "str_name":                       "",
                       "haus_nr_kompl":                  "",
                       "plz":                            "",
                       "ort_name":                       "",
                       "land_cdi":                       ""}
    
    nvp_mapping = {"CHANNEL":      "", 
                   "CAMPAIGNNAME": "", 
                   "TARGETGROUP":  ""}
                   
    nvp_mapping = [("CHANNEL",      ""), 
                   ("CAMPAIGNNAME", ""), 
                   ("TARGETGROUP",  ""),
                   ("SUBCATEGORY",  ""),
                   ("SUBCATEGORY",  "")]
    
      
    Usage Example (recommended to create a new colunm for the API result): 
    df["API_RESULT"] = df.apply(lambda row: soa_wrapper_marketinginteraktiondatenpush_1(row, 
                            interaction_mapping, contact_mapping, nvp_mapping, "DEV"), axis = 1)
    """ 
        
    interaction_dict = soa_dictmapper(row, interaction_mapping)
    contact_dict     = soa_dictmapper(row, contact_mapping)
    nvp_dict         = soa_dictmapper(row, nvp_mapping)    
    
    return soa_marketinginteraktiondatenpush_1(interaction_dict, contact_dict, nvp_dict,
                                               header = soa_createsoapheader(), stage = stage)

#%% Interface-specific Functions 
def soa_partnerfuzzysucheget_1(input_dict, header = soa_createsoapheader(), stage = "DEV"): 
    """Initiates a SOA service call to the API "PartnerFuzzySucheGet_1". With this service, partner can be found with a
    fuzzy-matching-algorith by uniserv. See the SOA documentation for more details on this webservice: 
    https://soa.wgrintra.net/ch/architecture/services/PartnerFuzzySucheGet_1/PartnerFuzzySucheGet_1.pdf    

    Owner: Manuel Kuhn

    Keyword Arguments:
    input_dict       << dict >>
                        Description of the supported key-value-pairs can be found here:
                        https://soa.wgrintra.net/ch/architecture/services/PartnerFuzzySucheGet_1/PartnerFuzzySucheGet_1.pdf

    header           << str (optional) >>
                        Header string for the SOA call. The default function creates an automated header 
                        string suitable for most service calls
    stage            << str (optional) >>
                        Defines the staging area on which to performm the call. Possible values include
                        "DEV", "ACC", and "PROD". 
    
    Return Value:
    << str >>
       String containing either an exception or the HTTP status code of the API request. 
       "OK" indicates that the API call was done successfully.
       
    Example:
        input_dict = {"mand_cdu":           "1",
                      "kz_vtrg_ausgb":      "false",
                      "kz_part_kng_ausgb":  "false",
                      "kz_gref_asg":        "false",
                      "max_row_value":      "1",
                      "vname":              "",
                      "nname":              "Haag" 
                     }  
                            
        status, response = soa_partnerfuzzysucheget_1(input_dict)
        """    
       
    
    try: 
        #A) Parameter Setting & Function Definition     
        stage_dict = {"DEV":  ["https://soadev.ch.winterthur.com:8443/PartnerFuzzySucheGet_1", 
                               r"{}\CCDA\09_Betrieb\00_Files\test-client-soapui.pem".format(sf.platform_is_server("drive"))],
                      "ACC":  ["https://soaacc.ch.winterthur.com:8443/PartnerFuzzySucheGet_1", 
                               r"E:\certs\client\sas_server_keystore.pem"],
                      "PROD": ["https://soaprod.ch.winterthur.com:8443/PartnerFuzzySucheGet_1", 
                               r"E:\certs\client\sas_server_keystore.pem"]}      

        # xml namespace
        PREFIX = "tns"
                                      
        # define all possible nodes in XML and hierarchy (parent)  
        # !!! order of elements matters (according to documentation)
                               #  elementname       regex (None if no text is needed)                      parent
        list_xml_definition = [["lpsoi076",          None,                                                  "-"],
                               ["mand_cdu",           "^[0-9]{1,3}$",                                        "lpsoi076"],
                               ["ausgb_awahl",        None,                                                  "lpsoi076"],
                               ["kz_vtrg_ausgb",       "^(?i)(true|false)$",                                 "ausgb_awahl"],
                               ["kz_part_kng_ausgb",   "^(?i)(true|false)$",                                 "ausgb_awahl"],
                               ["kz_gref_asg",        "^(?i)(true|false)$",                                  "ausgb_awahl"],
                               ["max_row_value",      "^[0-9]{1,3}$",                                        "ausgb_awahl"],
                               ["gpart_such",         None,                                                  "lpsoi076"],
                               ["np_such",            None,                                                  "gpart_such"],
                               ["nname",              "^[0-9a-zA-Z\-]{1,40}$",                               "np_such"],
                               ["vname",              "^[0-9a-zA-Z\-]{1,40}$",                               "np_such"],
                               ["geb_dat",            "([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))",   "np_such"],
                               ["jp_such",            None,                                                  "gpart_such"],
                               ["name_nnp_zeile1",    "^[0-9a-zA-Z\-]{1,40}$",                               "jp_such"],
                               ["name_nnp_zeile2",    "^[0-9a-zA-Z\-]{1,40}$",                               "jp_such"],
                               ["adr_post_such",      None,                                                  "gpart_such"],
                               ["gpart_typ_cdgpd",    "^[0-9]{1,3}$",                                        "adr_post_such"],
                               ["str_name_such",      "^[0-9a-zA-Z\-]{1,60}$",                               "adr_post_such"],
                               ["haus_nr_kompl_such", "^[0-9a-zA-Z\-]{1,10}$",                               "adr_post_such"],
                               ["plz_such",           "^[0-9a-zA-Z\-]{1,10}$",                               "adr_post_such"],
                               ["ort_name_such",      "^[0-9a-zA-Z\-]{1,40}$",                               "adr_post_such"],
                               ["land_cdi",           "^[0-9a-zA-Z\-]{1,2}$",                                "adr_post_such"],
                              ]
        
        
        # convert list of lists from above to list of dicts                      
        list_of_dicts_xml_definition = [{"elementname": i[0], "regex": i[1], "parent": i[2]} for i in list_xml_definition]
        
        #B) Generate list (keys_to_keep) of xml-nodes that must be created
        parents = {i[0]: i[2] for i in list_xml_definition}   
                    
        keys_to_keep = []
        for i in list_of_dicts_xml_definition:#[::-1]: # iterate reverse over list
            if i["elementname"] in input_dict.keys():
                tmp_elementname = i["elementname"]
                keys_to_keep.append(tmp_elementname)
                while tmp_elementname != "-":
                    tmp_elementname = parents[tmp_elementname]
                    keys_to_keep.append(tmp_elementname)
        keys_to_keep = list(set(keys_to_keep))
        
        #C) Generation inner XML-String 
        element_dict = {}

        element_dict[list_of_dicts_xml_definition[0]["elementname"]] = ET.Element("{0}:{1}".format(PREFIX, list_of_dicts_xml_definition[0]["elementname"]))
        
        for element in list_of_dicts_xml_definition[1:]:
            if ( element["regex"] is None or element["elementname"] in input_dict.keys() ) and (element["elementname"] in keys_to_keep):
                element_dict[element["elementname"]] = ET.SubElement(element_dict[element["parent"]], '{0}:{1}'.format(PREFIX, element["elementname"]))
                if element["regex"] is not None and element["elementname"] in input_dict.keys():
                    element_dict[element["elementname"]].text = input_dict[element["elementname"]]
                    
        data = ET.tostring(element_dict[list_of_dicts_xml_definition[0]["elementname"]]).decode("UTF-8")
            
        #G) Generation full XML-String
        data = """<?xml version="1.0" encoding="UTF-8"?>
                  <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mar="http://soa.wgrintra.net/ch/architecture/MarketingInteraktionDatenPush_1">
                      {0}
                  <soapenv:Body>
                  <{1}:search_partner_by_ausgb_awahl xmlns:{1}="http://soa.wgrintra.net/ch/architecture/PartnerFuzzySucheGet_1">
                  {2}
                  </{1}:search_partner_by_ausgb_awahl>
                  </soapenv:Body>
                  </soapenv:Envelope>""".format(header, PREFIX, data).encode("utf-8")
         
        #H) POST-Request Execution
        response = soa_apicall(method = "post", url = stage_dict[stage][0], data = data, cert = stage_dict[stage][1])
        
    except Exception:
        status = "Error: {}".format(traceback.format_exc())
        
    return response
    
def soa_wrapper_partnerfuzzysucheget_1(row, input_mapping, stage = "DEV"):
    """Wrapper for soa_marketinginteractiondatenpush_1 allowing to perform multiple SOA service 
    calls from a pd.DataFrame. The mapping  parameters map the required input parameters of the SOAP service 
    to DataFrame columns.

    Owner: Manuel Kuhn

    Keyword Arguments:
    row                 << pd.Series >>
                           Series containing all the values of a single DataFrame row. 
                           Best retrieved by: df.apply(lambda row: ...)
                           
    input_mapping       << dict >>
                           Mapping for the interaction_dict parameter of soa_marketinginteraktiondatenpush_1.
                           Keys include the possible parameters of the SOA service (str), 
                           the values the mapped column names (str) of the pd.DataFrame.
                           Please note that no fix values can be provided with this parameter; 
                           the dict values will always try to reference a pd.DataFrame column. 
                            
    stage               << str (optional) >>
                           Defines the staging area on which to performm the call. Possible values include
                           "DEV", "ACC", and "PROD". 
    
    Return Value:
    << str >>
       String containing either an exception or the HTTP status code of the API request. 
       "OK" indicates that the API call was done successfully.
       
    Example:
        import pandas as pd
        df = pd.DataFrame([{"Vorname": "Manuel", "Nachname": "Kuhn"},
                           {"Vorname": "Tobias", "Nachname": "Ippisch"}])
        
        df["mand_cdu"] =            "1"
        df["kz_vtrg_ausgb"] =       "false"
        df["kz_part_kng_ausgb"] =   "false"
        df["kz_gref_asg"] =         "false"
        df["max_row_value"] =       "1"
        
       

        input_mapping       =   {"mand_cdu":           "mand_cdu",
                                 "kz_vtrg_ausgb":      "kz_vtrg_ausgb",
                                 "kz_part_kng_ausgb":  "kz_part_kng_ausgb",
                                 "kz_gref_asg":        "kz_gref_asg",
                                 "max_row_value":      "max_row_value",
                                 "vname":              "Vorname",
                                 "nname":              "Nachname" 
                                 }  
                            
        df["API_RESPONSE"] = df.apply(lambda row: soa_wrapper_partnerfuzzysucheget_1(row, input_mapping, "DEV"), axis=1)
    
              
    Usage Example (recommended to create a new colunm for the API result): 
    df["API_RESULT"] = df.apply(lambda row: soa_wrapper_marketinginteraktiondatenpush_1(row, 
                            interaction_mapping, contact_mapping, nvp_mapping, "DEV"), axis = 1)"""    
    
    input_dict = soa_dictmapper(row, input_mapping)  
    
    return soa_partnerfuzzysucheget_1(input_dict, header = soa_createsoapheader(), stage = stage)    