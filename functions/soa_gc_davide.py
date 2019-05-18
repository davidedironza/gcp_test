# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 12:45:19 2018

@author: C126632
"""
import traceback
from datetime import datetime, timezone
from re       import match, DOTALL
from html     import escape
from requests import get, post
from requests.packages.urllib3 import disable_warnings

#import sys
#import shlex, subprocess


def soa_wrapper_marketinginteraktiondatenpush_1(row, interaction_mapping, contact_mapping, header, url, auth, headers,
                                                nvp_mapping = {}):
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
                            interaction_mapping, contact_mapping, nvp_mapping, "DEV"), axis = 1)"""    
    
    interaction_dict = soa_dictmapper(row, interaction_mapping)
    contact_dict     = soa_dictmapper(row, contact_mapping)
    nvp_dict         = soa_dictmapper(row, nvp_mapping)    
    
    return soa_marketinginteraktiondatenpush_1(interaction_dict, contact_dict, header, url, auth, headers, nvp_dict)




def soa_marketinginteraktiondatenpush_1(interaction_dict, contact_dict, header, url, auth, headers, nvp_dict = {}): 
    """Initiates a SOA service call to the API "MarketingInteraktionDatenPush_1". With this service, interactions and contacts can be 
    provided to Hybris-Marketing (yMKT) for use in marketing campaigns. See the SOA documentation for more details on this webservice: 
    http://soa.wgrintra.net/ch/architecture/services/template/inhalt.htmls?/ch/architecture/services/MarketingInteraktionDatenPush_1/    

    Owner: Tobias Ippisch
    Adapted for Datalake: Reto Haag

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

    
    Return Value:
    << str >>
       String containing either an exception or the HTTP status code of the API request. 
       "OK" indicates that the API call was done successfully."""    
    
    try: 
        #A) Parameter Setting & Function Definition     
        #stage_dict = {"DEV":  ["https://soadev.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       r"{}\CCDA\09_Betrieb\00_Files\test-client-soapui.pem".format(sf.platform_is_server("drive"))],
        #              "ACC":  ["https://soaacc.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       r"E:\certs\client\sas_server_keystore.pem"],
        #              "PROD": ["https://soaprod.ch.winterthur.com:8443/MarketingInteraktionDatenPush_1", 
        #                       r"E:\certs\client\sas_server_keystore.pem"]}                                                
              
        def aux_asserter(input_list, input_dict): 
            for i in input_list:
                x = input_dict.get(i[0], None)
                if ~(x is None) : assert(match(i[2], x, DOTALL)), "{} does not match format {}".format(i[0], i[2])
                
        def aux_generator(input_list, input_dict):
            output_string = ""
            for i in input_list: 
                x = input_dict.get(i[0], None)
                if (~(x is None)) & (x != "") :
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
        #for i in interaction_list[:2]: assert(i[0] in interaction_dict.keys()), "{} required in interaction_dict".format(i)       
        #aux_asserter(interaction_list, interaction_dict)
        
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
        #for i in contact_list[:2]: assert(i[0] in contact_dict.keys()), "{} required in contact_dict".format(i)        
        #aux_asserter(contact_list, contact_dict)
                
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
        
        #xsd_file = "{}/CCDA/09_Betrieb/00_Files/MarketingInteraktionDatenPush_1.xsd".format(sf.platform_is_server("drive"))  
        
        #if soa_validateschema(data, xsd_file) == False:
            #raise AssertionError("XML schema validation failed for: {}".format(data))
            
        #G) XML-String Generation 
        data = """<?xml version="1.0" encoding="UTF-8"?>
                  <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:mar="http://soa.wgrintra.net/ch/architecture/MarketingInteraktionDatenPush_1">
                      {0}
                  <soapenv:Body>{1}</soapenv:Body>
                  </soapenv:Envelope>""".format(header, data).encode("utf-8")
    
        #H) POST-Request Execution
        response = soa_apicall(method = "post", url = url, data = data, auth = auth, headers = headers)
        status = response.reason
        print(data)
    except Exception:
        status = "Error: {}".format(traceback.format_exc())
        print(status)
    return status




def soa_apicall(method, url, data, auth, headers, raise_exception = True):
    """Performs a standadized SOA service call. Supports both GET and POST method.  

    Owner: Tobias Ippisch 
    Adapted for Datalake: Reto Haag 

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
    if   method == "get":  response = get( url = url, data = data, auth = auth, headers = headers, verify = False)
    elif method == "post": response = post(url = url, data = data, auth = auth, headers = headers, verify = False)
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
    
    
    