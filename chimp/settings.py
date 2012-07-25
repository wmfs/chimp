import sys
import argparse
import psycopg2
import logging.handlers
import os
import cs
import xml.etree.ElementTree as etree
import chimpspec
import xml.dom.minidom

    
    
class Settings:

    class Zone:

        def __init__(self, zoneTag):
            self.id = cs.grabAttribute(zoneTag, "id")
            self.schema = cs.grabAttribute(zoneTag, "schema")
            self.table = cs.grabAttribute(zoneTag, "table")
            self.column = cs.grabAttribute(zoneTag, "column")
                  
    class Db:

        class Connection:

            def makeCursor(self, name, namedCursor, dictionaryCursor):
                if not namedCursor:
                    if not dictionaryCursor:
                        # False / False
                        newCursor = self.connection.cursor()
                    else:
                        # False / True
                        newCursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                else:
                    if not dictionaryCursor:
                        # True / False
                        newCursor = self.connection.cursor(name)
                    else:
                        # True / True
                        newCursor = self.connection.cursor(name, cursor_factory=psycopg2.extras.DictCursor)

                return(newCursor)

            def __init__(self, connectString):
                self.connection = psycopg2.connect(connectString)

        def __init__(self, configRoot, configName):
            self.configName = configName
            self.connectionFile = os.path.join(configRoot, "connections", configName + ".xml")
            xmlDoc = etree.parse(self.connectionFile).getroot()
            credentialsDocument = xmlDoc.attrib
            self.credentials = {}

            params = []
            for thisCredential in  credentialsDocument.items():
                if thisCredential[0] not in("vendor", "label", "version"):
                    self.credentials[thisCredential[0]] = thisCredential[1]
                    param = "%s=%s" % (thisCredential[0], thisCredential[1])
                    params.append(param)

            self.connectString = cs.delimitedStringList(params, " ")

        def makeConnection(self, label):
            connectString = self.connectString + " application_name=Chimp(%s)" % (label)
            return (self.Connection(connectString))

    def __init__(self):

        def getSettingValuesFromXmlFile(filename):
            env = {}
            visibilityLevels = {}
            securityLevels = {}
            xmldoc = xml.dom.minidom.parse(filename)
            settingsTag = xmldoc.getElementsByTagName("settings")[0]
            
            registryTag = settingsTag.getElementsByTagName("registry")[0]
            keyTags = registryTag.getElementsByTagName("key")
            for key in keyTags:
                name = cs.grabAttribute(key, "name")
                value = cs.grabAttribute(key, "value")
                env[name] = value
            
            visibilityLevelsTag = settingsTag.getElementsByTagName("visibilityLevels")[0]
            levelTags =  visibilityLevelsTag.getElementsByTagName("level")
            for level in levelTags:
                name = cs.grabAttribute(level, "name")
                value = int(cs.grabAttribute(level, "value"))
                visibilityLevels[name] = value

            securityLevelsTag = settingsTag.getElementsByTagName("securityLevels")[0]
            levelTags = securityLevelsTag.getElementsByTagName("level")
            for level in levelTags:
                name = cs.grabAttribute(level, "name")
                value = int(cs.grabAttribute(level, "value"))
                securityLevels[name] = value            

            zones=[]
            zoneTag = settingsTag.getElementsByTagName("zones")[0]         
            zonesTags = zoneTag.getElementsByTagName("zone")
            for zone in zonesTags:
                zones.append(self.Zone(zone))            
            return(env, visibilityLevels, securityLevels, zones)



#        resourceRoot = os.path.abspath("../../resources")
        self.paths = {}

        #locate config directory
#        self.paths["config"] = os.path.abspath("../config")
        #Get the parent directory of the current file, this avoids us having to hard code file paths
        #As this may change, depending on how you execute the application
        #This almost certainly isn't the best way to do this...
        self.paths["parentDir"] = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
        self.paths["config"] = os.path.join(self.paths["parentDir"], "config")
        self.paths["temp"] = os.path.join(self.paths["parentDir"], "temp")
        self.paths["resources"] = os.path.join(self.paths["parentDir"], "resources")

        self.paths["sqlTemplates"] = logFile = os.path.join(self.paths["resources"], "templates", "sql")
        self.paths["specificationTemplates"] = logFile = os.path.join(self.paths["resources"], "templates", "specification")
        self.paths["repositoryTemplates"] = logFile = os.path.join(self.paths["resources"], "templates", "repository")        
        
        # Load environment settings from settings.xml
        filename = os.path.join(self.paths["config"], "settings.xml")
        (self.env, self.visibilityLevels, self.securityLevels, self.zones) = getSettingValuesFromXmlFile(filename)

        self.paths["repository"] = self.env["repositoryPath"]

        # Well, if you thought that was bad, feast on this lot:
        self.paths["generatedScriptsDir"] = os.path.join(self.paths["repository"], "scripts", "generated", "specification files", "{0}")     
        self.paths["generatedSQLScriptsDir"] = os.path.join(self.paths["generatedScriptsDir"], "sql")
        self.paths["buildSQLFile"] = os.path.join(self.paths["generatedSQLScriptsDir"], "install", "build_{0}.sql".format("{0}"))
        self.paths["dropSQLFile"] = os.path.join(self.paths["generatedSQLScriptsDir"], "install", "drop_{0}.sql".format("{0}"))
        self.paths["generatedIndexesSQLScriptsDir"] = os.path.join(self.paths["generatedSQLScriptsDir"], "indexes")
        self.paths["generatedImportSQLScriptsDir"] = os.path.join(self.paths["generatedSQLScriptsDir"], "import")            
        self.paths["generatedMVSQLScriptsDir"] = os.path.join(self.paths["generatedSQLScriptsDir"], "mv")
        self.paths["generatedValidationSQLScriptsDir"] = os.path.join(self.paths["generatedSQLScriptsDir"], "validation")
        self.paths["generatedPythonScriptsDir"] = os.path.join(self.paths["generatedScriptsDir"], "py")
        self.paths["generatedTransformationPythonScriptsDir"] = os.path.join(self.paths["generatedPythonScriptsDir"], "transformation")
        self.paths["generatedCalculatedPythonScriptsDir"] = os.path.join(self.paths["generatedPythonScriptsDir"], "calculated")
        self.paths["generatedSolrFormatterScriptsDir"] = os.path.join(self.paths["generatedPythonScriptsDir"], "solr formatting")


        # Establish logging
        logFile = os.path.join(self.env["logPath"], "chimp_log.txt")
        self.appLogger = logging.getLogger('logger')
        handler = logging.handlers.RotatingFileHandler(logFile, maxBytes=int(self.env["logMaxBytes"]), backupCount=int(self.env["logBackupCount"]))
        formatter = logging.Formatter(self.env["logFormat"])
        handler.setFormatter(formatter)
        self.appLogger.addHandler(handler)
        if self.env["loggingLevel"]=="debug": 
            self.appLogger.setLevel(logging.DEBUG)            
        elif self.env["loggingLevel"]=="info": 
            self.appLogger.setLevel(logging.INFO)
        elif self.env["loggingLevel"]=="warning":
            self.appLogger.setLevel(logging.WARNING)
        elif self.env["loggingLevel"]=="error":
            self.appLogger.setLevel(logging.ERROR)
        elif self.env["loggingLevel"]=="critical":
            self.appLogger.setLevel(logging.CRITICAL)        

        self.appLogger.debug("")
        self.appLogger.debug("")
        self.appLogger.debug("")
        self.appLogger.info("========")
        self.appLogger.info("STARTING")  
        self.appLogger.info("========")
        self.appLogger.debug("Args (pre-parse):")
        self.appLogger.debug(sys.argv[1:])   
        self.appLogger.debug("")       
        # Parse arguments
        parser = argparse.ArgumentParser(description="A tool for loading and processing data")        
        subParsers = parser.add_subparsers(dest="command")
        
        installParser = subParsers.add_parser("install", help="Install Chimp schemas")
        installParser.add_argument("--zones", action="store_true", help="S")
        installParser.add_argument("--dbconnection", action="store", help="S")

        buildParser = subParsers.add_parser("build", help="For generating scripts")
        buildParser.add_argument("--specification", action="store", help="Specification for which scripts will be built")
        buildParser.add_argument("--solrserver", action="store", help="Solr server for which scripts will be built")
        buildParser.add_argument("--dbconnection", action="store", help="S")
        buildParser.add_argument("--install", action="store_true", help="S")
        buildParser.add_argument("--drop", action="store_true", help="S")
        buildParser.add_argument("--reinstall", action="store_true", help="S")

        stageParser = subParsers.add_parser("import", help="For staging, importing and absorbing external data")
        stageParser.add_argument("--streamname", action="store", default="normal", help="S")
        stageParser.add_argument("--specification", action="store", help="Specification to import data into")
        stageParser.add_argument("files", nargs="*", help="Defines individual files or groups")
        stageParser.add_argument("--limit", action="store", type=int, help="Limit records staged")
        stageParser.add_argument("--importmode", action="store", default="auto", choices=("auto","full","change","sync"), help="S")
        stageParser.add_argument("--tolerancelevel", action="store", default="error", choices=("none", "warning","error","exception"), help="S" )
        stageParser.add_argument("--commitfrequency", action="store", default="major", choices=("minor","major"), help="S" )
        stageParser.add_argument("--checkpointbehaviour", action="store", default="tolerate", choices=("tolerate", "commit","rollback"), help="S" )
        stageParser.add_argument("--dbconnection", action="store", help="S")
        stageParser.add_argument("--deferprocessing", action="store_true", help="S")
        stageParser.add_argument("--recurse", action="store_true", help="Recurse directories")
        stageParser.add_argument("--filenameregex", action="store", help="Regular expression to help filter filenames")
        stageParser.add_argument("--groupid", action="store", help="S")
        stageParser.add_argument("--json", action="store", help="S")
        stageParser.add_argument("--vacuumstrategy", action="store", default="none", choices=("none", "progressive", "aggressive"), help="S")
        stageParser.add_argument("--postimportcompute", action="store", choices=("none", "specification", "full"), default="specification", help="S")
        
        
        computeParser = subParsers.add_parser("compute", help="For refreshing things independently of any import")
        computeParser.add_argument("--streamname", action="store", default="normal", help="S")
        computeParser.add_argument("--specificationrestriction", action="store", dest="specificationrestriction", help="Restrict computation to these [comma delimited] specifications")
        computeParser.add_argument("--restriction", action="store", help="A comma separated list of elements that should be calculated... [custom|ctree|pins|search]")
        computeParser.add_argument("--deferprocessing", action="store_true", help="S")
        computeParser.add_argument("--dbconnection", action="store", help="S")
        computeParser.add_argument("--groupid", action="store", help="S")
        computeParser.add_argument("--tolerancelevel", action="store", default="error", choices=("none", "warning","error","exception"), help="S" )
        computeParser.add_argument("--commitfrequency", action="store", default="major", choices=("minor","major"), help="S" )
        computeParser.add_argument("--checkpointbehaviour", action="store", default="tolerate", choices=("tolerate", "commit","rollback"), help="S" )
                
        queueParser = subParsers.add_parser("queue", help="For dealing with queue issues")        
        queueParser.add_argument("--action", action="store", choices=("clear","restart","stop"), help="Clear the queue")
        queueParser.add_argument("--streamname", action="store", default="normal", help="Stream name")
        queueParser.add_argument("--dbconnection", action="store", help="S")
        
        cleanParser = subParsers.add_parser("clean", help="Drop all Chimp schemas")
        cleanParser.add_argument("--dbconnection", action="store", help="S")
        cleanParser.add_argument("--force", action="store_true", help="S")
        
        createParser = subParsers.add_parser("create", help="Create a Chimp entity")
        createParser.add_argument("--entitytype", choices=["specification", "solrserver"], help="The entity type")
        createParser.add_argument("--name", action="store", help="The name of the entity")

        extractParser = subParsers.add_parser("extract", help="Extract data out of Chimp")
        extractParser.add_argument("--specification", action="store", help="Specification to import data from")
        extractParser.add_argument("--format", help="Format that the extracted data should take")        
        extractParser.add_argument("--limit", action="store", help="Record limit of extracted output")
        


        toolParser = subParsers.add_parser("tool", help="Run one of the misc tools provided by Chimp")    
        toolParser.add_argument("--name", action="store", help="Name of tool")
        toolParser.add_argument("--profile", action="store", help="Some tools support the use of ready-made param profiles")
        toolParser.add_argument("--dbconnection", action="store", help="Database connection (if appropriate)")

        args = parser.parse_known_args()
        self.args = args[0]    
        self.extraArgs = args[1]

        self.appLogger.info("  env              : %s" %(str(self.env)))
        self.appLogger.info("  args             : %s" %(str(self.args)))        
        self.appLogger.info("  extraArgs        : %s" %(str(self.extraArgs)))
        self.appLogger.info("  visibilityLevels : %s" %(str(self.visibilityLevels)))
        self.appLogger.info("  securityLevels   : %s" %(str(self.securityLevels)))        
        self.appLogger.info("  Zones")
        for zone in self.zones:
            self.appLogger.info("    {0}: {1}.{2} ({3})".format(zone.id, zone.schema, zone.table, zone.column))
      
        # Load specification if provided
        if hasattr(self.args, "specification") and self.args.specification is not None:   
            sendSpecToLog = True
            if self.args.command in("import","compute"):
                sendSpecToLog = False 
            self.specification = chimpspec.Spec(self, self.args.specification, sendSpecToLog)
        else:
            self.specification = None
            
        # Connect to database if required        
        if hasattr(self.args,"dbconnection") and self.args.dbconnection is not None:
            self.db = self.Db(self.paths["config"], self.args.dbconnection)
        else:
            self.db = None
