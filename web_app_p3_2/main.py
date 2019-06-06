# -*- coding: utf-8 -*-
# Modul-Import
import os
import tornado.web
import tornado.ioloop

from pytz import timezone
from datetime import datetime

print(__name__)

class IndexClass(tornado.web.RequestHandler):
    def get(self):
        now = datetime.now(timezone('Europe/Zurich'))
        response = "It is now {0} GMT".format(now.strftime("%d.%m.%Y %H:%M:%S"))
        self.write(response)


# Initialize App
def make_app():
    return tornado.web.Application([(r"/", IndexClass)], debug=False)

print(os.environ['PORT'])
print("APP Main")
app = make_app()