'''
Created on 4 Mar 2012

@author: Tim.Needham
'''
import cs

class SearchEntry:
    '''
    classdocs
    '''


    def __init__(self, searchEntryTag):
        self.type = "searchEntry"
        self.taskOrder = 60
        self.domain= cs.grabAttribute(searchEntryTag,"domain")
        self.ranking= cs.grabAttribute(searchEntryTag,"ranking")
    
    def debug(self, appLogger):
        appLogger.debug("    searchEntry")
        appLogger.debug("      domain  : {0}".format(self.domain))
        appLogger.debug("      ranking : {0}".format(self.ranking))

    def getExtraSystemFields(self):
        extraSystemFields = []
        return(extraSystemFields)
    
    def requiresFile(self):
        return(True)  

    def getFunctionScript(self, sourceFields):
        body= "???\n"
        return(body)
    
    def getTriggeringColumns(self):
        return([])                           
        