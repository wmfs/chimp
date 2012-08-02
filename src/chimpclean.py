'''
Created on 8 Dec 2011

@author: Ryan Pickett
'''

import chimputil

def clean(settings, force=False):
    if force or chimputil.confirm():
        
        supportConnection = settings.db.makeConnection("support")
        supportCursor = supportConnection.makeCursor("supportCursor", False, False)
        
        supportCursor.execute("DROP SCHEMA IF EXISTS history, pinhead, search, "
                              "shared, store, working, mv, reference, ctree, "
                              "stage, import, editable, vc CASCADE")
        
        supportConnection.connection.commit()
        supportConnection.connection.close();  
    else:
        
        exit(0)  