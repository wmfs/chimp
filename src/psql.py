'''
Created on 12 Jan 2012

@author: Ryan Pickett
'''

import subprocess

class PSQLExecutor:
    def __init__(self, settings):
        self.settings = settings
        
    def execute(self, filename):
        subprocess.call([self.settings.env["psqlExecutable"],
                         "-h{0}".format(self.settings.db.credentials["host"]),
                         "-U{0}".format(self.settings.db.credentials["user"]),
                         "-p{0}".format(self.settings.db.credentials["port"]),
                         "-f{0}".format(filename),
#                         "-a",
                         self.settings.db.credentials["dbname"]])
        