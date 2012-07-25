'''
Created on 9 Dec 2011

@author: Ryan Pickett
'''

import chimpscript
import calc.solr as solr
from extract import Builder as extractBuilder
from psql import PSQLExecutor
#from chimpsql import DBObjectRegistry
#from codegen import SpecificationScriptBuilder 


def chimpSchemaExists(settings):    
    supportConnection = settings.db.makeConnection("support")
    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
    
    supportCursor.execute("select count(*) from information_schema.tables "
          "where table_schema = 'shared' and table_name = 'specification_registry'")
    result = supportCursor.fetchone()[0] == 1
    
    supportConnection.connection.commit()
    supportConnection.connection.close();
    
    return result
          
    
def buildSpecificationScripts(settings, install, drop):    
    chimpscript.makeBuildScript(settings)
    eb = extractBuilder.Builder(settings)
    eb.debug(settings.appLogger)
    eb.generateAllExtractorScripts()
      
    if drop:
        PSQLExecutor(settings).execute(settings.paths["dropSQLFile"].format(settings.specification.name))
    if install:        
        PSQLExecutor(settings).execute(settings.paths["buildSQLFile"].format(settings.specification.name))

def buildSolrServerScripts(settings, serverName, install, drop):
    appLogger = settings.appLogger
    repositoryPath = settings.paths["repository"]
    configPath = settings.paths["config"]

    solrSettings = solr.SolrSettings(settings)
    solrSettings.debug(appLogger)
    
    solrFields = solr.SolrFields(settings)
    solrFields.debug(appLogger)
    
    solrServer = solr.SolrServer(settings, serverName, solrFields)
    solrServer.debug(appLogger)

    solrServer.generateSolrConfig(solrSettings, configPath, repositoryPath, appLogger)
    solrServer.generateDataConfig(solrSettings, solrFields, repositoryPath, appLogger)
    solrServer.generateSchema(solrSettings, solrFields, configPath, repositoryPath, appLogger)
    (dropFilename,installFilename) = solrServer.generateInstallScript(repositoryPath, solrSettings, solrFields)


    #chimpscript.makeBuildScript(settings)
    #solrServer.makeBuildScript(solrSettings, solrFields, repositoryPath, appLogger)
    
    #searchDomain = search.SearchDomain()
    #searchDomain.setFromFile(settings, searchDomainName)
    #searchDomain.debug(settings)
    
#    buildScriptResult = searchDomain.makeBuildScript(settings, "file")        

    if drop:
        PSQLExecutor(settings).execute(dropFilename)
    if install:        
        PSQLExecutor(settings).execute(installFilename)
