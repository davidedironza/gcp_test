
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
from html import unescape
from random import randint
from google.cloud import bigquery
from google.cloud import storage
import sys

#0.3 Pfadinformationen
path_data_va = "bindexis/data/various/"
path_data_input = "bindexis/data/input/"

# 1   Retrieve Bindexis Data
#1.1 Access & Authentication Setup
print("1.1")
username      = "TIppisch"
time_now      = datetime.datetime.now(pytz.timezone('Europe/Zurich'))

client_cs = storage.Client()
bucket_cs = client_cs.get_bucket(BUCKET)

#3.2 Speicherung Parameter & Backup bei erfolgreichem Lauf
print("3.2")
blob = bucket_cs.blob(path_data_va+'Write_TimeNow1.txt')
blob.upload_from_string(str(time_now))

sys.exit(1)