#import psycopg2
import psycopg2.extras
import psycopg2.extensions
from xml.dom import minidom
import getpass
import re
import os.path

def getChimpScriptFilenameToUse(repositoryPath, folders, filename):

    # Does file exist in the "custom" branch?
   
#    fullPath = os.path.join(repositoryPath, "scripts", "custom")
#    for thisFolder in folders:
#        fullPath = os.path.join(fullPath, thisFolder)
#    fullPath = os.path.join(fullPath, filename)
#    
#    if not os.path.exists(fullPath):
#        fullPath = os.path.join(repositoryPath, "scripts", "generated")
#        for thisFolder in folders:
#            fullPath = os.path.join(fullPath, thisFolder)
#        fullPath = os.path.join(fullPath, filename)

    fullPath = repositoryPath
    for thisFolder in folders:
        fullPath = os.path.join(fullPath, thisFolder)
    customPath = os.path.join(fullPath, "custom", filename)
    fullPath = os.path.join(fullPath, filename)

    if os.path.exists(customPath):
        r = customPath
    else:
        r = fullPath

    return(r)

def addSlashes(text):
    if text is not None:
        specials=[("\b", "\\b"), ("\t", "\\t"), ("\n", "\\n"), ("\a", "\\a"), ("\r", "\\r")]
        for thisSpecial in specials:
            text.replace(thisSpecial[0],thisSpecial[1])
    return(text)


def titleCase(s):
    return re.sub(r"[A-Za-z]+('[A-Za-z]+)?",
    lambda mo: mo.group(0)[0].upper() +mo.group(0)[1:].lower(),s)


def simpleText(s):
    if s is not None:
        s=s.lower()
        s=s.replace(","," ")
        s=s.replace("-"," ")
        s=s.replace("*"," ")
        s=s.replace("|"," ")
        s=s.replace("'"," ")
        s=s.replace("("," ")
        s=s.replace(")"," ")
    return(s)

def searchText(s):
    return(simpleText(s))

def removePrefix(s):    
    i = s.find("_")+1
    r = s[i:]
    return(r)
    
def getOrderByComponents(s, maxIntergerValue, maxStringLength, firstNumberDefault):    
    components=[]
    components.append( firstNumberDefault)    
    components.append('ZZZZZZ')
    components.append( firstNumberDefault)
    components.append('ZZZZZZ')
    components.append( firstNumberDefault)
    components.append('ZZZZZZ')
    components.append( firstNumberDefault)
    components.append('ZZZZZZ')
    split = re.split('([0-9]+)', simpleText(s))
    
    if s !="":
        i =0  
        insertPosition=0
        while i<9 and i<len(split):        
            if i==0:
                if split[0]=="":
                    insertPosition=0
                    i=1
                else:
                    insertPosition=1
                    
            if insertPosition < 8:       
                if i%2==1:   
                    numberString=split[i].strip()
                    if numberString !="":
                        number=int(numberString)
                        if number > maxIntergerValue:
                            number=maxIntergerValue
                            
                        components[insertPosition]=number
                
                else:
                    if split[i]==" ":
                        charString=split[i]
                    else:
                        charString=split[i].strip()
                    if charString != "":
                        components[insertPosition]=charString[0:maxStringLength]
                
            i=i+1
            insertPosition=insertPosition+1
            
    return(components);    

def prettyNone(text):
    if text is None:
        sorted="[None]"
    else:
        sorted=text
    return(sorted)

class ChimpError(Exception):
    def __init__(self, errorCode, message):
        self.errorCode = errorCode
        self.msg = message
        
    def __str__(self):
        info="["+prettyNone(str(self.errorCode))+"]: "+prettyNone(self.msg)
        return info


def logError(errorCode, lineNumber, message, lineData, raiseError, appLogger):
    appLogger.error("")
    appLogger.error("*************************************************************************")
    appLogger.error("ERROR ["+str(prettyNone(errorCode))+"]:")
        
    if message is not None:
        appLogger.error("  Message     : "+str(message))
        
    if lineNumber is not None:
        appLogger.error("  Line number : "+str(lineNumber))
            
    if lineData is not None:
        appLogger.error("  Line        : "+lineData)
        appLogger.error("*************************************************************************")
        appLogger.error("")
    
    if raiseError:
        msg=message
        if lineNumber is not None:
            msg=msg+" on line "+str(lineNumber)
        msg=msg+" (see log)"
        raise ChimpError(errorCode,msg)


def grabAttribute(element, name):
    if name in element.attributes:
        r=element.attributes[name].value
    else:
        r=None
        
 #       print(element.attributes["hello"]) 
 #   if element.attributes.has_key(name):
 #       r=element.attributes[name].value
  #  else:
   #     r=None
    return(r);


def grabTag(element, tagName):
    tagValue=None
    t = element.getElementsByTagName(tagName)
    if t.length>0:
        if t[0].childNodes.length>0:
            tagValue=t[0].childNodes[0].data
    return(tagValue)


def delimitedStringList(list, delimiter):
    delimitedString=None
    if list is not None:
        delimitedString=""
        i=0
        for thisString in list:
            i=i+1
            delimitedString=delimitedString+thisString
            if i<len(list):
                delimitedString=delimitedString+delimiter
    return(delimitedString)


def prefixedDelimitedStringList(list, prefix, delimiter):
    delimitedString=None
    if list is not None:
        delimitedString=""
        i=0
        for thisString in list:
            i=i+1
            delimitedString=delimitedString+prefix+thisString
            if i<len(list):
                delimitedString=delimitedString+delimiter
    return(delimitedString)

def prefixedAndSuffixedDelimitedStringList(list, prefix, suffix, delimiter):
    delimitedString=None
    if list is not None:
        delimitedString=""
        i=0
        for thisString in list:
            i=i+1
            delimitedString=delimitedString+"%s%s%s" %(prefix, thisString, suffix)
            if i<len(list):
                delimitedString=delimitedString+delimiter
    return(delimitedString)


class Db:

    def __init__(self, resourceRoot, connectionName, userOverride, passwordOverride, appLogger):

        def addConnectionParameter(s, name, value):
            r=s
            if value is not None:
                if s is None:
                    r=name+"="+value
                else:
                    r=r+" "+name+"="+value
            return(r)
    
        def getDbApi2ConnectString():
            s=None
    
            if self.vendor=="PostgreSQL":
                s=addConnectionParameter(s,"dbname",self.dbname)
                s=addConnectionParameter(s,"database",self.database)
                s=addConnectionParameter(s,"user",self.user)
                s=addConnectionParameter(s,"password",self.password)
                s=addConnectionParameter(s,"host",self.host)
                s=addConnectionParameter(s,"port",self.port)
                s=addConnectionParameter(s,"sslmode",self.sslmode)
            return(s)


        #Load connection file...
        
        self.connectionFile = os.path.join(resourceRoot,"connections", connectionName+".xml")

        self.connectionName = connectionName

        xmldoc = minidom.parse(self.connectionFile)
        connectionTag = xmldoc.getElementsByTagName("connection")[0]

        self.label=grabAttribute(connectionTag,"label")

        self.vendor=grabAttribute(connectionTag,"vendor")

        self.version=grabAttribute(connectionTag,"version")
        
        self.dbname=grabAttribute(connectionTag,"dbname")

        self.database=grabAttribute(connectionTag,"database")

        self.host=grabAttribute(connectionTag,"host")

        self.port=grabAttribute(connectionTag,"port")

        self.sslmode=grabAttribute(connectionTag,"sslmode")

        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("Connection details")
            appLogger.debug("------------------")
            appLogger.debug("  connectionFile: "+prettyNone(self.connectionFile))
            appLogger.debug("  connectionName: "+prettyNone(self.connectionName))
            appLogger.debug("  label         : "+prettyNone(self.label))
            appLogger.debug("  vendor        : "+prettyNone(self.vendor))
            appLogger.debug("  version       : "+prettyNone(self.version))
            appLogger.debug("  dbname        : "+prettyNone(self.dbname))
            appLogger.debug("  database      : "+prettyNone(self.database))
            appLogger.debug("  host          : "+prettyNone(self.host))        
            appLogger.debug("  port          : "+prettyNone(self.port))        
            appLogger.debug("  sslmode       : "+prettyNone(self.sslmode))



        self.user=None
        self.password=None

        #Derive user...
        if userOverride is None:
            self.user=grabAttribute(connectionTag,"user")
        else:
            self.user=userOverride
        if self.user is None:
            self.user=input("User ("+str(self.label)+") : ")

        #Derive password...
        if passwordOverride is None:
            self.password=grabAttribute(connectionTag,"password")
        else:
            self.password=passwordOverride
        if self.password is None:
            self.password=getpass.getpass("Password ("+str(self.label)+") : ")
        
        #Now connect...
        if self.vendor=="PostgreSQL":
            self.connectString = getDbApi2ConnectString()
            if appLogger is not None:
                appLogger.debug("  ConnectString     : "+prettyNone(self.connectString))
            

            self.supportConnection = psycopg2.connect(self.connectString)
            self.support = self.supportConnection.cursor()
            
            self.dataConnection = psycopg2.connect(self.connectString)
            self.data = self.dataConnection.cursor()

        xmldoc.unlink()

    def createNamedCursor(self):
        self.namedCursor =self.supportConnection.cursor("changes",cursor_factory=psycopg2.extras.DictCursor)

    def createDictionaryCursor(self):
        self.dictionaryCursor =self.supportConnection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def createNamedDictionaryCursor(self):
        self.namedDictionaryCursor =self.supportConnection.cursor("changes",cursor_factory=psycopg2.extras.DictCursor)

    def closeNamedCursor(self):
        self.namedCursor.close()

    def closeDictionaryCursor(self):
        self.dictionaryCursor.close()

    def closeNamedDictionaryCursor(self):
        self.namedDictionaryCursor.close()
    
    def closeConnections(self): 
        self.support.close()
        self.supportConnection.close()
        self.data.close()
        self.dataConnection.close()
    
    def resetSupportIsolationLevel(self): 
        self.supportConnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def resetDataIsolationLevel(self): 
        self.dataConnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
     

    def supportCommit(self):
        self.supportConnection.commit()

    def supportRollback(self):
        self.supportConnection.rollback()

    def dataCommit(self):
        self.dataConnection.commit()

    def dataRollback(self):
        self.dataConnection.rollback()
        