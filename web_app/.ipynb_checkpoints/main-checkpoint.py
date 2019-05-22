# -*- coding: utf-8 -*-
#Modul-Import
import os
import pickle
import tornado.web
import tornado.ioloop

from boto3                      import client
from pymysql                    import connect
from getpass                    import getuser 
from pytz                       import timezone
from datetime                   import datetime, date  
from traceback                  import format_exc
from selenium.common.exceptions import TimeoutException

if getuser() == "tobias": os.chdir("/home/tobias/PycharmProjects/2019-01_AWS/Comparis_Quotecheck")
if getuser() == "ubuntu": os.chdir("/home/ubuntu/python/Comparis_Quotecheck")

os.getcwd()
import crawler 

product_dict = {'7':  'baloisedirect.ch',
                '8':  'Vaudoise',
                '9':  'Vaudoise',        
                '10': 'smile.direct',
                '11': 'Zurich',
                '29': 'ELVIA',
                '30': 'smile.direct',
                '31': 'smile.direct',
                '33': 'Generali',
                '34': 'AXA',
                '35': 'AXA',
                '36': 'AXA',
                '37': 'AXA',
                '38': 'Generali',
                '39': 'Die Mobiliar',
                '40': 'Die Mobiliar',
                '44': 'Helvetia',
                '45': 'Helvetia',
                '46': 'Generali',
                '51': 'Generali',
                '52': 'Generali',
                '53': 'Generali',
                '56': 'ELVIA',
                '57': 'Allianz Suisse',
                '58': 'Zurich',
                '59': 'Zurich',
                '60': 'baloisedirect.ch',
                '61': 'baloisedirect.ch',
                '62': 'baloisedirect.ch',
                '63': 'Dextra',
                '64': 'Dextra',
                '65': 'Die Mobiliar',
                '66': 'Generali',
                '67': 'Generali',
                '68': 'Generali',
                '69': 'Allianz Suisse',
                '70': 'PostFinance',
                '72': 'PostFinance',
                '73': 'Allianz Suisse'}

mode_dict = {'a': 'Nachfass',
             'b': 'Marktanteil',
             'c': 'Random'}

class IndexClass(tornado.web.RequestHandler):
    def get(self):
        now = datetime.now(timezone('Europe/Zurich')) 
        response = "It is now {0} GMT".format(now.strftime("%d.%m.%Y %H:%M:%S"))
        self.write(response)     
        
class OfferClass(tornado.web.RequestHandler):
    def get(self):
        try:
            #0.1 Initialize Crawler & read GUID
            guid = self.get_argument("guid")
            now = datetime.now(timezone('Europe/Zurich'))  
            mode = mode_dict.get(self.get_argument("mode"), 'Andere')

            browser = crawler.Crawler(webbrowser = "ChromeHeadless")

            #0.2 Go to URL    
            url = "https://www.comparis.ch/autoversicherung/berechnen/result?inputguid={0}".format(guid)
            browser.goto(url)

            #1   Extract Offer Information    
            #1.1 Initialize result_dict
            result_dict = {}
            
            #1.2 Extract Offer Company & Product ID 
            xpath  = "//div[@data-wt-tr-event = 'productImpressions']"
            aux_list = browser.extract_elements(xpath, attribute = "data-wt-ec-productid")
            result_dict["OFFER_PRODUCTID"] = aux_list
            result_dict["OFFER_COMPANY"] = [product_dict.get(i, 'anderer Versicherer') for i in aux_list]

            #1.3 Write Fixed Information
            aux_len = len(aux_list)
            result_dict["OFFER_GUID"] = [guid           for i in range(aux_len)]
            result_dict["OFFER_MODE"] = [mode           for i in range(aux_len)]
            result_dict["OFFER_RANK"] = [i+1            for i in range(aux_len)] 
            result_dict["CRAWL_DATE"] = [now            for i in range(aux_len)]

            #1.4 Extract Resulttype of Offer (Top Preis-Leistung, Deckungstreffer)
            xpath  = ["//div[contains(@class, 'comparison-result show-for-small')]",
                     "/descendant::div[@class = 'row']",
                     "/descendant::span[contains(@class, 'label')]"]
            
            aux_list = browser.extract_elements("".join(xpath), attribute = 'textContent')
            aux_list = aux_list + ["" for i in range(aux_len - len(aux_list))]
            result_dict["OFFER_RESULTTYPE"] = aux_list

            #1.5 Extract Offer Visibility
            xpath  = ["//div[@class = 'comparison-result show-for-small']", 
                      "/descendant::div[@class = 'result-header row']", 
                      "/descendant::div[@class = 'columns small-2 provider']",  
                      "/descendant::h2[@class = 'sub-title3']",
                      "/descendant::a[@class = 'headline']",
                      "/descendant::strong"]

            aux_list = browser.extract_elements("".join(xpath), attribute = "textContent")
                        
            if len(aux_list) == aux_len: 
                aux_list = [False if "Angebot" in i else True for i in aux_list]
            else: 
                aux_list = [True for i in range(aux_len)]
            result_dict["OFFER_VISIBLE"] = aux_list 
            
            #1.6 Extract Offer Price
            xpath  = ["//div[@class = 'comparison-result show-for-small']", 
                      "/descendant::div[@class = 'result-header row']", 
                      "/descendant::div[@class = 'columns small-2 align-middle']", 
                      "/descendant::strong"] 
                            
            aux_list = browser.extract_elements("".join(xpath), attribute = "textContent")[::2]
            aux_list = [i.replace("CHF ", "").replace("'", "") for i in aux_list]                       
            aux_list = aux_list + [0.0 for i in range(aux_len - len(aux_list))]
            result_dict["OFFER_PRICE"] = aux_list 

            #1.7 Extract Quote Request Information
            xpath = "//a[@data-wt-fa-label = 'Click - request quote']"
            aux_list = browser.extract_elements(xpath, attribute = "textContent")[1::2]
            aux_list = [True if "Offerte bestellt" in i else False for i in aux_list]
            aux_list = aux_list + [False for i in range(aux_len - len(aux_list))]
            result_dict["OFFER_QUOTE_REQUESTED"] = aux_list 

            #1.8 Extract Defects
            def defect_extractor(x):
                if "text-red" in x:    return "DEFECT"
                if "exclamation" in x: return "WARNING"
                if "check" in x:       return "OK"
                else:                  return None

            xpath  = ["//div[@class = 'comparison-result show-for-small']", 
                      "/descendant::div[@class = 'result-header row']", 
                      "/descendant::div[@class = 'columns small-2']",
                      "/descendant::div[@class = 'row']"] 
            
            try:
                aux_list = browser.extract_elements("".join(xpath) + "/descendant::small", attribute = "textContent")
                aux_defects = browser.extract_elements("".join(xpath) + "/descendant::i", attribute = "class")
                aux_defects = [defect_extractor(i) for i in aux_defects]
            except TimeoutException:
                aux_list = []
                aux_defects = []

            for d in ["Bonusschutz", "Selbstbehalt", "Parkschaden", "Mitgeführte Sachen", "Vollkasko", "Teilkasko", "Haftpflicht"]:
                aux_index = [i for i, j in enumerate(aux_list) if j.strip() == d][:aux_len]
                aux_index = [j for i, j in enumerate(aux_defects) if i in aux_index]
                aux_index = aux_index + [None for i in range(aux_len - len(aux_index))]
                result_dict["OFFER_DEFECT_" + d.upper().replace(" ", "")] = aux_index



            #2   Create Response    
            #2.1 Extract Offers with Quote (AXA & Competitors)
            aux_index = [i for i, j in enumerate(result_dict["OFFER_QUOTE_REQUESTED"]) if j == True]

            #2.2 Construct JSON Response Success
            response = []
            for i, j in enumerate(aux_index):
                response += ['"QUOTE_%s": {"ANBIETER": "%s", "PREIS": "%s", "RANG": %s}' \
                %(i, result_dict["OFFER_COMPANY"][j], result_dict["OFFER_PRICE"][j], result_dict["OFFER_RANK"][j])]

            response = "{" + ", ".join(response) + "}"
            
        except Exception as e:
            #3   Exception Handling
            #3.1 Send Email
            exception_date = pickle.load(open("exception_date.pkl", "rb"))
            
            if exception_date != date.today():
                ses_client = client('ses', region_name='eu-west-1')
          
                _ = ses_client.send_email(
                    Destination = {'ToAddresses':  ['tobias.ippisch@gmail.com', 'tobias.ippisch@axa-winterthur.ch'],
                                   'CcAddresses':  [],
                                   'BccAddresses': []},
                    Source  =      'tobias.ippisch@gmail.com',
                    Message = {'Body':    {'Text': {'Charset': 'UTF-8',
                                                    'Data':     format_exc()}},
                               'Subject': {'Charset': 'UTF-8',
                                           'Data':    'Comparis-Crawling-API: Fehlermeldung'}})  
                
                pickle.dump(date.today(), open("exception_date.pkl", "wb"))
            
            #3.2 Construct Error result_dict
            aux_len = 1
            result_dict = {"CRAWL_DATE":  [now], "CRAWL_ERROR": [e], "OFFER_GUID":  [guid]} 
 
            #3.3 Construct JSON Response Error
            response = '{"Exception": "%s"}' %e

        finally: 
            #4   Cleanup and store into DB
            #4.1 Release browser and required resources
            browser.browser.quit()

            #4.2 Connect to SQLite Database
            rds_host = "crawling-db.ceeoyoyate8p.eu-central-1.rds.amazonaws.com"
            conn = connect(rds_host, user = "ippischtobi", passwd = "Absolutismus18!", db = "crawling_data")
            #conn = connect('database.db')
            cursor = conn.cursor()

            #4.3 (Optional) Initialize DB tables
            offer_list = [["CRAWL_DATE",                     "TIMESTAMP"],
                          ["OFFER_GUID",                     "VARCHAR(36)"],
                          ["OFFER_MODE",                     "VARCHAR(11)"],
                          ["OFFER_VISIBLE",                  "BOOL"],
                          ["OFFER_RESULTTYPE",               "VARCHAR(18)"],                         
                          ["OFFER_RANK",                     "INTEGER"],
                          ["OFFER_PRICE",                    "VARCHAR(16)"],
                          ["OFFER_QUOTE_REQUESTED",          "BOOL"],
                          ["OFFER_COMPANY",                  "VARCHAR(24)"],
                          ["OFFER_PRODUCTID",                "VARCHAR(2)"],
                          ["OFFER_DEFECT_BONUSSCHUTZ",       "VARCHAR(7)"],
                          ["OFFER_DEFECT_SELBSTBEHALT",      "VARCHAR(7)"],
                          ["OFFER_DEFECT_PARKSCHADEN",       "VARCHAR(7)"],
                          ["OFFER_DEFECT_MITGEFÜHRTESACHEN", "VARCHAR(7)"],
                          ["OFFER_DEFECT_HAFTPFLICHT",       "VARCHAR(7)"],                          
                          ["OFFER_DEFECT_TEILKASKO",         "VARCHAR(7)"],    
                          ["OFFER_DEFECT_VOLLKASKO",         "VARCHAR(7)"],                              
                          ["CRAWL_ERROR",                    "VARCHAR(32)"]] 

            #sql_reset = """DROP TABLE comparis_extractquote;
            #              CREATE TABLE comparis_extractquote({0})""".format(", ".join([i[0] + " " + i[1] for i in offer_list]))
            #cursor.execute(sql_reset)  

            #4.4 Send results to DB
            for j in range(aux_len):  
                sql_columns = []
                sql_values  = []

                for i in [l for l in offer_list if l[0] in result_dict.keys()]:
                    if result_dict[i[0]][j] != None: 
                        #Append relevant columns
                        sql_columns += [i[0]]  

                        #Appened relevant values
                        if i[1] in ["INTEGER", "FLOAT", "BOOL"]: 
                            sql_values += [str(result_dict[i[0]][j])]  
                        elif i[1] == "TIMESTAMP":                
                            sql_values += [''' "{0}" '''.format(result_dict[i[0]][j].strftime("%Y-%m-%d %H:%M:%S"))]                      
                        else:                                    
                            sql_values += [''' "{0}" '''.format(str(result_dict[i[0]][j]).replace('"','')[:int(i[1][8:-1]) ])]

                sql = """INSERT INTO comparis_extractquote ({0}) VALUES ({1})""".format(", ".join(sql_columns), ", ".join(sql_values))
                cursor.execute(sql) 

            #4.5 Commit Changes & Close Connection
            conn.commit()
            conn.close()

            #4.6 Respond
            self.write(response)       
        
        
        
#Initialize App
def make_app():
    return tornado.web.Application([(r"/",              IndexClass),
                                    (r"/extractquote",  OfferClass)])

if __name__ == "__main__":
    app = make_app()
    app.listen(5000)
    tornado.ioloop.IOLoop.current().start()