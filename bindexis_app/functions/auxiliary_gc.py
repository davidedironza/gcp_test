# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 12:45:19 2018

@author: C126632
"""

# Modul-Import
import random
import re



#Hilfunktion, um Spaltenbezeichnungen und entsprechende Werte in einen String umzuwandeln und 
# und ihn mit Zeilenumbruch darzustellen (Ziel: schöne Darstellung im CRM)
def col_concatenator(row, c1, c2, c3, c4, c5, c6):
    result = ""
    for z in [c1, c2, c3, c4, c5, c6]: 
        if (type(row[z]) == str) & (row[z] != ""): result = result + z + ": " + row[z] + "\n"
    return result

#Hilfsfunktion, um via Zufallszahl die Kontrollgruppe zu bilden
def kg_assigner(kg_share):
    ran = random.random()
    if kg_share <= ran:  return 1
    else:                return 0 

    
#%% 3)
    
def send_email(subject, body, to, cc = "", bcc = "", sender = "ccda@axa-winterthur.ch", 
               attachment = "", use_amg = False):
    """Wrapper function for sending eMails via MS Outlook Application or AXA Application Mail Gateway (AMG, on Server only)
    Owner: Tobias Ippisch
    Keyword Arguments:
    subject     << str >>
                   eMail subject
    body        << str >>
                   eMail body, either in html or plain format
    to          << list, str >>
                   eMail receipients, separated by '; '
    cc          << list, str (optional) >>
                   eMail cc receipients, separated by '; '
    bcc         << list, str (optional) >>
                   eMail bcc receipients, separated by '; '
    attachment  << str (optional) >>
                   File path to (single) attachment                   
    use_amg     << bool (optional) >>
                   Specifies whether mails should be sent by AXA Application Mail Gateway
                   (AMG, only available on server) or local mail client (e.g. MS Outlook)
                   
    Return Value:
    None
    """
    if use_amg==True or os.name == 'posix':
        #Package Import
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        from email import encoders
        
        #Create & populate new mail object
        newMail = MIMEMultipart()
        
        newMail['Subject'] = subject
        newMail['From']    = sender
        newMail['To']      = "; ".join(to)  if type(to)  == list else to
        newMail['Cc']      = "; ".join(cc)  if type(cc)  == list else cc
        newMail['Bcc']     = "; ".join(bcc) if type(bcc) == list else bcc
        
        if "<html>" in body: newMail.attach(MIMEText(body, 'html')) 
        else:                newMail.attach(MIMEText(body, 'plain')) 
              
        if attachment != "": 
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(attachment, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(attachment)))
            newMail.attach(part)
             
        #Define receipient list
        receipient_list = []
        
        def receipient_creator(receipient_list, receipients):
            if type(receipients) == list: 
                receipient_list += receipients
            elif type(receipients) == str:
                receipient_list += [i.strip() for i in receipients.split(";") if i != ""]
            return receipient_list
        
        receipient_list = receipient_creator(receipient_list, to)
        receipient_list = receipient_creator(receipient_list, cc)
        receipient_list = receipient_creator(receipient_list, bcc)
        
        #Connet to AMG and send eMail
        #s = smtplib.SMTP(host = 'amg.medc.services.axa-tech.intraxa', port = 25)
        s = smtplib.SMTP(host = '10.140.60.6', port = 25)
        s.ehlo('axa-winterthur.ch')
        s.sendmail(sender, receipient_list, newMail.as_string())
        s.quit()

    elif(os.name == 'nt'):
        #Package Import
        import win32com.client
        #from win32com.client import Dispatch, constants

        #Create & populate new mail object
        olMailItem = 0x0
        obj = win32com.client.Dispatch("Outlook.Application")
        newMail = obj.CreateItem(olMailItem)

        newMail.To  = "; ".join(to ) if type(to)  == list else to
        newMail.CC  = "; ".join(cc)  if type(cc)  == list else cc
        newMail.BCC = "; ".join(bcc) if type(bcc) == list else bcc
        newMail.Subject = subject

        if attachment != "": newMail.Attachments.Add(attachment)
        
        if "<html>" in body: newMail.HTMLBody = body
        else: newMail.Body = body

        #Send Mail Object
        newMail.send
    return  

#%% 4.14)
def split_housenumber_street(x):
    """Splits a string with (potential) street / house number information into a tuple
    with separate street and house number

    Owner: Tobias Ippisch

    Keyword Arguments:
    x             << str >>
                     String with (potential) street / house number information; treats German address format
                     (house number at the end) and French address format (house number at the front) differently

    Return Value:
    tuple of strings (str street,
                      str house number)
    
    Examples:
    
    df['strasse'] = df.firma_strasse.apply(lambda x: sf.split_housenumber_street(x)[0] if pd.notnull(x) else None)
    df['hausnummer'] = df.firma_strasse.apply(lambda x: sf.split_housenumber_street(x)[1] if pd.notnull(x) else None)

    """

    aux_firstnumber = re.search("\d", x.rstrip(", "))
    if aux_firstnumber:
        aux_start = aux_firstnumber.start()
        if any(i in x.rstrip(", ")[aux_start:] for i in [",", " "]): #Franzoesisches Format
            aux_separator = min(i for i in [x.rstrip(", ").find(" "), x.rstrip(", ").find(",")] if i >= 0)
            aux_street = x.rstrip(", ")[aux_separator:]
            aux_house_no = x.rstrip(", ")[:aux_separator]

        else: #Deutsches Format
            aux_street = x.rstrip(", ")[:aux_start]
            aux_house_no = x.rstrip(", ")[aux_start:]
    else:
        aux_street = x
        aux_house_no = ""

    return aux_street.rstrip(", ").lstrip(", "), aux_house_no.rstrip(", ").lstrip(", ")


    