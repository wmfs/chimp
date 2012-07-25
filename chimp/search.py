import os
import xml.dom.minidom
import cs
import imp
from chimputil import BuildScriptResult

class ContributingField():
    def __init__(self, tag, column, type, mandatory):
        self.column = column
        self.type = type
        self.mandatory = mandatory
        self.prefix=cs.grabAttribute(tag, "prefix")
        self.suffix=cs.grabAttribute(tag, "suffix")
        self.delimiter=cs.grabAttribute(tag, "delimiter")
        
        self.columnWithoutPrefix=self.column 
        i = self.columnWithoutPrefix.find("_")+1
        self.columnWithoutPrefix = self.columnWithoutPrefix[i:]


def expandRawValue(counter, column, type, prefix, suffix):
    
    needsStringConversion = ["integer","decimal","bigint","datetime","date","timestamp with time zone"]

    r=""
        
    if type in needsStringConversion:
        r=r+"str("
    
    if prefix is not None:
        r=r+"\"%s\" + " %(cs.addSlashes(prefix))    
         
    r = r + "raw[\"%s\"]" %(column)

    if suffix is not None:
        r=r+" + \"%s\"" %(cs.addSlashes(suffix))    


    if type in needsStringConversion:
        r=r+")"

    return(r)


def constructBasicConcatenationScript(assembler):
    indent="\t\t"

    if len(assembler.attributes)>0:
        i=0        
        script=indent+"parts=[]\n"
        for thisAttribute in assembler.attributes: 
            
            if thisAttribute.mandatory:
    
                if i>0 and (assembler.delimiter is not None or thisAttribute.delimiter):                    
                    script = script + indent +"if len(parts)>0:\n"                     
                    if thisAttribute.delimiter is not None:
                        delimiter = thisAttribute.delimiter
                    else:
                        delimiter=assembler.delimiter
                    script = script + indent +"\tparts.append(\"%s\")\n" %(cs.addSlashes(delimiter))
    
                expanded = expandRawValue(i, thisAttribute.column, thisAttribute.type, thisAttribute.prefix, thisAttribute.suffix) 
                script=script+ indent +"parts.append(%s)\n" %(expanded)
            else:
                script = script + indent + "if raw[\"%s\"] is not None:\n" %(thisAttribute.column)
                
                if i>0 and (assembler.delimiter is not None or thisAttribute.delimiter):                    
                    script = script + indent +"\tif len(parts)>0:\n"                     
                    if thisAttribute.delimiter is not None:
                        delimiter = thisAttribute.delimiter
                    else:
                        delimiter=assembler.delimiter
                    script = script + indent +"\t\tparts.append(\"%s\")\n" %(cs.addSlashes(delimiter))
                                        
                expanded = expandRawValue(i, thisAttribute.column, thisAttribute.type, thisAttribute.prefix, thisAttribute.suffix)
                
                script = script + indent + "\tparts.append(%s)\n" %(expanded)                
            i=i+1 
        
        script = script + indent + "if len(parts) > 0:\n"
        script = script + indent + "\tr=parts[0]\n"
        script = script + indent + "\tfor i in range(1,len(parts)):\n"
        script = script + indent + "\t\tr=r+parts[i]\n"
        script = script + indent + "else:\n"
        script = script + indent + "\tr=None\n"
        
        if hasattr(assembler,'prefix') and hasattr(assembler,'suffix'):
             
            if assembler.prefix is not None and assembler.suffix is None:
                script=script+indent+"r = \"%s\" + r\n" %(cs.addSlashes(assembler.prefix))
            elif assembler.prefix is None and assembler.suffix is not None:
                script=script+indent+"r = r + \"%s\"\n" %(cs.addSlashes(assembler.suffix))
            elif assembler.prefix is not None and assembler.suffix is not None:
                script=script+indent+"r = \"%s\" + r + \"%s\"\n" %(cs.addSlashes(assembler.prefix),cs.addSlashes(assembler.suffix))
        
        script=script+indent+"return (r)\n\n"
    
    else:
        script=indent+"return(None)\n\n"
    return(script)


class LabelAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    LabelAssembler")
            appLogger.debug("    --------------")            
            appLogger.debug("    prefix      : '%s'" %(cs.prettyNone(self.prefix)))            
            appLogger.debug("    suffix      : '%s'" %(cs.prettyNone(self.suffix)))            
            appLogger.debug("    delimiter   : '%s'" %(cs.prettyNone(self.delimiter)))
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.prefix=cs.grabAttribute(tag, "prefix")
            self.suffix=cs.grabAttribute(tag, "suffix")
            self.delimiter=cs.grabAttribute(tag, "delimiter")
            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")[0]
            contributingFields=contributingFieldsTag.getElementsByTagName("field")
            
            for thisContributingField in contributingFields:
                column = cs.grabAttribute(thisContributingField, "column")
                for thisAttribute in metaAttributes:
                    if thisAttribute.column == column:                        
                        attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                        self.attributes.append(attribute)

    def getDefaultScript(self):
        script = constructBasicConcatenationScript(self)    
        return(script)
    

class SearchAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    SearchAssembler")
            appLogger.debug("    ---------------")            
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            if len(contributingFieldsTag) > 0:
                contributingFieldsTag = contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)

    def getDefaultScript(self):               
        i=0
        indent="\t\t"
        
        if len(self.attributes)>0:
            script=indent+"r=None\n"
            for thisAttribute in self.attributes:                
                expanded = expandRawValue(i, thisAttribute.column, thisAttribute.type, thisAttribute.prefix, thisAttribute.suffix)
                if thisAttribute.mandatory:
                    if i==0:
                        script=script+indent + "r=%s\n" %(expanded)
                    else:
                        script=script+indent + "r=r+%s\n" %(expanded)                    
                else:
                    
                    if i==0:
                        script=script+indent + "if raw[\"%s\"] is not None:\n" %(thisAttribute.column)                    
                        script=script+indent + "\tr=%s\n" %(expanded)
                    else:
                        script=script+indent + "if raw[\"%s\"] is not None:\n" %(thisAttribute.column)
                        script=script+indent + "\tr=r+\" \"+%s\n" %(expanded)                    
                
                i=i+1
                
            script=script+indent + "return(r)\n\n"  
        else:
            script = indent+"return(entry.label)\n\n"
            
        return(script)

class SynopsisAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    SynopsisAssembler")
            appLogger.debug("    -----------------")            
            appLogger.debug("    prefix      : '%s'" %(cs.prettyNone(self.prefix)))            
            appLogger.debug("    suffix      : '%s'" %(cs.prettyNone(self.suffix)))            
            appLogger.debug("    delimiter   : '%s'" %(cs.prettyNone(self.delimiter)))
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.prefix=cs.grabAttribute(tag, "prefix")
            self.suffix=cs.grabAttribute(tag, "suffix")
            self.delimiter=cs.grabAttribute(tag, "delimiter")
            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")[0]
            contributingFields=contributingFieldsTag.getElementsByTagName("field")
            
            for thisContributingField in contributingFields:
                column = cs.grabAttribute(thisContributingField, "column")
                for thisAttribute in metaAttributes:
                    if thisAttribute.column == column:
                        attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                        self.attributes.append(attribute)

    def getDefaultScript(self):
        script = constructBasicConcatenationScript(self)    
        return(script)

class ClassificationAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    ClassificationAssembler")
            appLogger.debug("    -----------------------")            
            appLogger.debug("    prefix      : '%s'" %(cs.prettyNone(self.prefix)))            
            appLogger.debug("    suffix      : '%s'" %(cs.prettyNone(self.suffix)))            
            appLogger.debug("    delimiter   : '%s'" %(cs.prettyNone(self.delimiter)))
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.prefix=cs.grabAttribute(tag, "prefix")
            self.suffix=cs.grabAttribute(tag, "suffix")
            self.delimiter=cs.grabAttribute(tag, "delimiter")
            self.attributes=[]
            contributingFields=tag.getElementsByTagName("contributingFields")
            
            if len(contributingFields)>0:
                            
                contributingFieldsTag =contributingFields[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)

    def getDefaultScript(self):
        script = constructBasicConcatenationScript(self)    
        return(script)

class CtreeAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    CtreeAssembler")
            appLogger.debug("    --------------")            
            appLogger.debug("    table             : '%s'" %(cs.prettyNone(self.table)))
            appLogger.debug("    ancestorColumn    : '%s'" %(cs.prettyNone(self.ancestorColumn)))
            appLogger.debug("    descendantColumn  : '%s'" %(cs.prettyNone(self.descendantColumn)))                          
            appLogger.debug("")
            
    def __init__(self, tag, metaAttributes):

        if tag is not None:            
            self.table=cs.grabAttribute(tag, "table")
            self.ancestorColumn=cs.grabAttribute(tag, "ancestorColumn")
            self.descendantColumn=cs.grabAttribute(tag, "descendantColumn")

    def getFunctionName(self):
        r = "get_%s_node_placement" %(self.table)
        return(r)

    def getDefaultScript(self, urnColumn):

        indent = "\t\t"   
        if self.table is not None and self.ancestorColumn is not None and self.descendantColumn is not None:
            
            script =          indent + "sql = \"select * from ctree.%s" %(self.getFunctionName())
            script=script+"(%s)\"\n"
            script = script + indent + "dbCursor.execute(sql, (raw[\"%s\"],))\n" %(self.descendantColumn)
            script = script + indent + "nodePlacement=dbCursor.fetchone()\n"
            script = script + indent + "return((nodePlacement[0],nodePlacement[1],nodePlacement[2],nodePlacement[3]))\n\n"
        else:            
            if urnColumn is not None:
                script = indent + "return((0,raw[\"%s\"],raw[\"%s\"],0))\n\n" %(urnColumn, urnColumn)
            else:
                script = indent + "return((0,None,None,0))\n\n"
         
        return(script)
                        

class UrnAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    UrnAssembler")
            appLogger.debug("    ------------")            
            appLogger.debug("    column        : '%s'" %(cs.prettyNone(self.column)))
            appLogger.debug("    urnLabelPrefix: '%s'" %(cs.prettyNone(self.urnLabelPrefix)))                         
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.column=cs.grabAttribute(tag, "column")
            self.urnLabelPrefix=cs.grabAttribute(tag, "urnLabelPrefix")
            
            if self.column is not None:
                for thisAttribute in metaAttributes:
                    if thisAttribute.column == self.column:
                        self.type = thisAttribute.type
                        self.mandatory = thisAttribute.mandatory
            else:
                self.type = None
                self.mandatory = None
                    
    def getDefaultScript(self):
            
        indent="\t\t"
        if self.column is not None:
            script =          indent + "r=raw[\"%s\"]\n" %(self.column)   
            script = script + indent + "return(r)\n"
        else:
            script =          indent + "return(None)\n"
            
            
        script = script + indent + "\n"
    
        return(script)

class IconAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    IconAssembler")
            appLogger.debug("    ------------")            
            appLogger.debug("    fixedIconName            : '%s'" %(cs.prettyNone(self.fixedIconName)))
            appLogger.debug("    column                   : '%s'" %(cs.prettyNone(self.column)))
            appLogger.debug("    pinheadIconGeneratorName : '%s'" %(cs.prettyNone(self.pinheadIconGeneratorName)))
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.column=cs.grabAttribute(tag, "column")
            self.fixedIconName=cs.grabAttribute(tag, "fixedIconName")            
            self.pinheadIconGeneratorName=cs.grabAttribute(tag, "pinheadIconGeneratorName")

            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            if len(contributingFieldsTag) > 0:
                contributingFieldsTag = contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)                                    

            
                                
    def getDefaultScript(self):
            
        indent="\t\t"
        
        if self.fixedIconName is not None:
            script =         indent + "# iconInfo contains a fixed icon name to use...\n"    
            script = script+ indent + "return (iconInfo)\n"    
                
        elif self.pinheadIconGeneratorName is not None:
            script =          indent + "# iconInfo contains a dictionary for conversion...\n"
            script = script + indent + "v=str(raw[\"%s\"])\n" %(self.column)
            script = script + indent + "if v is not None:\n"
            script = script + indent + "\tr = iconInfo[v][0]\n"
            script = script + indent + "else:\n"
            script = script + indent + "\tr = None\n" 
            script = script + indent + "return(r)\n"   
            
        elif len(self.attributes)>0:
            script = constructBasicConcatenationScript(self)
            
        script = script + indent + "\n"
    
        return(script)

class ResultScriptAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    ResultScriptAssembler")
            appLogger.debug("    ---------------------")            
            appLogger.debug("    prefix      : '%s'" %(cs.prettyNone(self.prefix)))            
            appLogger.debug("    suffix      : '%s'" %(cs.prettyNone(self.suffix)))            
            appLogger.debug("    delimiter   : '%s'" %(cs.prettyNone(self.delimiter)))
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.prefix=cs.grabAttribute(tag, "prefix")
            self.suffix=cs.grabAttribute(tag, "suffix")
            self.delimiter=cs.grabAttribute(tag, "delimiter")
            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            
            if len(contributingFieldsTag)>0:
                contributingFieldsTag=contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)

    def getDefaultScript(self):
        if len(self.attributes)>0:
            script = constructBasicConcatenationScript(self)
        else:
            script = "\t\treturn(None)\n\n"    
        return(script)



class FilterAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    FilterAssembler")
            appLogger.debug("    --------------")            
            appLogger.debug("    fixed       : '%s'" %(cs.prettyNone(self.fixed)))            
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        
        if tag is not None:
            self.fixed=cs.grabAttribute(tag, "fixed")

            self.attributes=[]
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            
            if len(contributingFieldsTag)>0:
                contributingFieldsTag=contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)


    def getDefaultScript(self):
        indent = "\t\t"   
        if self.fixed is not None:        
            script = indent + "r=\"%s\"\n"  %(self.fixed)   
            script = script + indent + "return(r)\n\n"
        elif len(self.attributes)>0:   
            script = constructBasicConcatenationScript(self)    
        return(script)
    


class PositionalAccuracyAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    PositionalAcuracyAssembler")
            appLogger.debug("    --------------------------")            
            appLogger.debug("    fixed       : '%s'" %(cs.prettyNone(self.fixed)))            
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        if tag is not None:
            self.attributes=[]
            self.fixed=cs.grabAttribute(tag, "fixed")
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            if len(contributingFieldsTag) > 0:
                contributingFieldsTag = contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)                                    
        

    def getDefaultScript(self):
        indent = "\t\t"   
        
        if self.fixed is not None:        
            script = indent + "r=%d\n"  %(int(self.fixed))               
        else:
            script = indent + "r=raw[\"%s\"]\n" %(self.attributes[0].column)
        script = script + indent + "return(r)\n\n"         
        return(script)


class LabelRankingAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    LabelRankingAssembler")
            appLogger.debug("    ---------------------")            
            appLogger.debug("    fixed       : '%s'" %(cs.prettyNone(self.fixed)))            
            appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        
        if tag is not None:
            self.fixed=cs.grabAttribute(tag, "fixed")

            self.attributes=[]            
            contributingFieldsTag =tag.getElementsByTagName("contributingFields")
            
            if len(contributingFieldsTag) >0:
                contributingFieldsTag = contributingFieldsTag[0]
                contributingFields=contributingFieldsTag.getElementsByTagName("field")
                
                for thisContributingField in contributingFields:
                    column = cs.grabAttribute(thisContributingField, "column")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == column:
                            attribute=ContributingField(thisContributingField, thisAttribute.column, thisAttribute.type,thisAttribute.mandatory)
                            self.attributes.append(attribute)



    def getDefaultScript(self):
        indent = "\t\t"   
        if self.fixed is not None:        
            script = indent + "r=%d\n"  %(int(self.fixed))   
            script = script + indent + "return(r)\n\n"
        else:
            script = indent + "r=1\n"   
            script = script + indent + "return(r)\n\n"                     
        return(script)

class CoordinatesAssembler:
                        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("    CoordinatesAssembler")
            appLogger.debug("    --------------------")  
            for thisPair in self.coordinatePairs:
                appLogger.debug("    Pair" )
                appLogger.debug("      xColumn  : %s" %(cs.prettyNone(thisPair[0])))            
                appLogger.debug("      yColumn  : %s" %(cs.prettyNone(thisPair[1])))
                appLogger.debug("")
                
    def __init__(self, tag, metaAttributes):
        self.coordinatePairs=[]
        contributingFieldsTag =tag.getElementsByTagName("contributingFields")
        if tag is not None:
            if len(contributingFieldsTag) >0:
                contributingFieldsTag =contributingFieldsTag[0]
                coordinatePairs=contributingFieldsTag.getElementsByTagName("coordinatePair")
    
                self.coordinatePairs=[]
                for coordinatePair in coordinatePairs:
                    suppliedXColumn = cs.grabAttribute(coordinatePair, "xColumn")
                    suppliedYColumn = cs.grabAttribute(coordinatePair, "yColumn")
                    for thisAttribute in metaAttributes:
                        if thisAttribute.column == suppliedXColumn:
                            xColumn = thisAttribute.column
                        elif thisAttribute.column == suppliedYColumn:
                            yColumn = thisAttribute.column
                    self.coordinatePairs.append((xColumn,yColumn))

    def getDefaultScript(self):
        indent = "\t\t"   
         
        if len(self.coordinatePairs)>0:
            coordinatePair = self.coordinatePairs[0]
            xColumn = coordinatePair[0]
            yColumn = coordinatePair[1]
            script=indent + " return ((raw[\"%s\"], raw[\"%s\"]))\n\n" %(xColumn, yColumn)
        else:
            script=       indent + " x = 0  # Needs changing\n"
            script=script+indent + " y = 0\n"
            script=script+indent + " return ((x,y))\n\n" 
        return(script)
    

class SearchAttribute():
    
    def __init__(self, isBaseTable, attributeTag, fields, primaryKeyColumns, appLogger):
        
        #All set for the big change, no?
        self.column = cs.grabAttribute(attributeTag,"column")
        
#        self.suppliedColumnName = cs.grabAttribute(attributeTag,"column")
        
#        if isBaseTable:
#            if self.suppliedColumnName in(primaryKeyColumns):
#                self.actualColumnName = "pk_%s" %(self.suppliedColumnName)
#            else:
#                self.actualColumnName = "w_%s" %(self.suppliedColumnName)
#        else:
#            self.actualColumnName = self.suppliedColumnName 

#        if self.suppliedColumnName in(primaryKeyColumns):
#            self.actualColumnName = "pk_%s" %(self.suppliedColumnName)
#        else:
#            self.actualColumnName = "w_%s" %(self.suppliedColumnName)

        
        for thisField in fields:             
            if thisField.column == self.column:  
                self.type = thisField.dumpDataType
                self.mandatory = thisField.mandatory
                
                
#        if appLogger is not None:
#            appLogger.debug("")
#            appLogger.debug("      %s:" %(self.suppliedColumnName))
#            appLogger.debug("        actualColumnName  : %s:" %(self.actualColumnName))
#            appLogger.debug("        type              : %s:" %(self.type))
#            appLogger.debug("        mandatory         : %s:" %(str(self.mandatory)))
        
        

class Search():
    def __init__(self, isBaseTable, searchTag, fields, primaryKeyColumns, appLogger):
        self.enabled = False        

        if searchTag is not None:
            if len(searchTag)>0:
                
                self.enabled=True
                self.searchDomain=None
                
                if appLogger is not None:
                    appLogger.debug("")
                    appLogger.debug("Search")
                    appLogger.debug("------")
                    
                searchTag = searchTag[0]
                self.attributes = []
                self.searchDomain =str(cs.grabAttribute(searchTag,"domain"))
                
                self.ranking =cs.grabAttribute(searchTag,"ranking")
                if self.ranking is not None:
                    self.ranking =int(self.ranking)
                else:
                    self.ranking=1

                if appLogger is not None:
                    appLogger.debug("  searchDomain  : %s"%(str(self.searchDomain)))
                    appLogger.debug("  ranking       : %d"%(self.ranking))
            
                allContributingAttributesTag = searchTag.getElementsByTagName("contributingFields")[0]
                allAttributes = allContributingAttributesTag.getElementsByTagName("field")
                
                for attributeTag in allAttributes:                                    
                    attribute = SearchAttribute(isBaseTable, attributeTag, fields, primaryKeyColumns, appLogger)
                    self.attributes.append(attribute)
                
                defaultContentAssemblersTag = searchTag.getElementsByTagName("defaultContentAssemblers")[0]


                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("urnAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.urnAssembler = UrnAssembler(assemblerTag, self.attributes)

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("iconAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.iconAssembler = IconAssembler(assemblerTag, self.attributes)
                
                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("labelAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.labelAssembler = LabelAssembler(assemblerTag, self.attributes)
                        
                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("searchAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.searchAssembler = SearchAssembler(assemblerTag, self.attributes)    
                
                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("filterAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.filterAssembler = FilterAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("coordinatesAssembler")                
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.coordinatesAssembler = CoordinatesAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("synopsisAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.synopsisAssembler = SynopsisAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("resultScriptAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.resultScriptAssembler = ResultScriptAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("positionalAccuracyAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.positionalAccuracyAssembler = PositionalAccuracyAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("labelRankingAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.labelRankingAssembler = LabelRankingAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("classificationAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.classificationAssembler = ClassificationAssembler(assemblerTag, self.attributes)    

                assemblerTag = defaultContentAssemblersTag.getElementsByTagName("ctreeAssembler")
                if len(assemblerTag)>0:
                    assemblerTag=assemblerTag[0] 
                    self.ctreeAssembler = CtreeAssembler(assemblerTag, self.attributes)    
                
# =====================================================================================================
            
                if appLogger is not None:  
                    
                    if hasattr(self, 'urnAssembler'):     
                        self.urnAssembler.debug(appLogger)
                    
                    if hasattr(self, 'labelAssembler'):
                        self.labelAssembler.debug(appLogger)
                    
                    if hasattr(self, 'searchAssembler'):
                        self.searchAssembler.debug(appLogger)
                    
                    if hasattr(self, 'filterAssembler'):
                        self.filterAssembler.debug(appLogger)
                    
                    if hasattr(self, 'synopsisAssembler'):
                        self.synopsisAssembler.debug(appLogger)
                    
                    if hasattr(self, 'coordinatesAssembler'):
                        self.coordinatesAssembler.debug(appLogger)

                    if hasattr(self, 'ctreeAssembler'):
                        self.ctreeAssembler.debug(appLogger)
            

class SearchEntry():

    def __init__(self, recordId):
        self.recordId = recordId
        self.filter = None
        self.recordRanking = None
        self.label= None
        self.search = None
        self.synopsis = None
        self.classification = None
        self.resultScript = None
        self.zone = None
        self.x = None
        self.y = None
        self.urn = None
        self.icon = None
        self.positionalAccuracy = None
        self.visibility = None
        self.security = None
        self.labelRanking = None
        self.depth = None
        self.immediateAncestor= None
        self.rootAncestor = None
        self.descendantCount =None
    
    def setRecordId(self, recordId):
        self.recordId = recordId

    def setFilter(self, filter):
        self.filter = filter
    
    def setRecordRanking(self, recordRanking):
        self.recordRanking = recordRanking
    
    def setLabel(self, label):
        self.label = label
    
    def setSearch(self, search):
        self.search = search
    
    def setSynopsis(self, synopsis):
        self.synopsis = synopsis
    
    def setClassification(self, classification):
        self.classification = classification

    def setCtree(self, depth, immediateAncestor, rootAncestor, descendantCount):
        self.depth = depth
        self.immediateAncestor = immediateAncestor
        self.rootAncestor = rootAncestor
        self.descendantCount = descendantCount
    
    def setResultScript(self, resultScript):
        self.resultScript = resultScript
    
    def setZone(self, zone):
        self.zone = zone
    
    def setUrn(self, urn):
        self.urn = urn

    def setIcon(self, icon):
        self.icon = icon
    
    def setCoordinates(self, x,y):
        self.x = x
        self.y = y
    
    def setPositionalAccuracy(self, positionalAccuracy):
        self.positionalAccuracy = positionalAccuracy
    
    def setVisibility(self, visibility):
        self.visibility = visibility
    
    def setSecurity(self, security):
        self.security = security
    
    def setLabelRanking(self, labelRanking):
        self.labelRanking = labelRanking
       
    def debug(self, appLogger):
        appLogger.debug("")
        appLogger.debug("Search entry")  
        appLogger.debug("------------")
        appLogger.debug("  urn                  : %s" %(str(self.urn)))
        appLogger.debug("  recordId             : %s" %(str(self.recordId)))
        appLogger.debug("  filter               : %s" %(str(self.filter)))
        appLogger.debug("  recordRanking        : %s" %(str(self.recordRanking)))
        appLogger.debug("  label                : %s" %(str(self.label)))
        appLogger.debug("  search               : %s" %(str(self.search)))
        appLogger.debug("  synopsis             : %s" %(str(self.synopsis)))
        appLogger.debug("  classification       : %s" %(str(self.classification)))
        appLogger.debug("  depth                : %s" %(str(self.depth)))
        appLogger.debug("  immediateAncestor    : %s" %(str(self.immediateAncestor)))
        appLogger.debug("  rootAncestor         : %s" %(str(self.rootAncestor)))
        appLogger.debug("  descendantCount      : %s" %(str(self.descendantCount)))            
        appLogger.debug("  resultScript         : %s" %(str(self.resultScript)))
        appLogger.debug("  zone                 : %s" %(str(self.zone)))
        appLogger.debug("  x                    : %s" %(str(self.x)))
        appLogger.debug("  y                    : %s" %(str(self.y)))
        appLogger.debug("  positionalAccuracy   : %s" %(str(self.positionalAccuracy)))
        appLogger.debug("  visibility           : %s" %(str(self.visibility)))
        appLogger.debug("  security             : %s" %(str(self.security)))
        appLogger.debug("  labelRanking         : %s" %(str(self.labelRanking)))

    def getTableRow(self, domainConfig, tableName):
        record = []
        record.append(self.recordId)
        record.append(tableName)
        if domainConfig.recordRanking:
            record.append(self.recordRanking)
        record.append(self.label)
        record.append(self.search)

        if domainConfig.urn:
            record.append(self.urn)

        if domainConfig.icon:
            record.append(self.icon)
                    
        if domainConfig.synopsis:
            record.append(self.synopsis)
            
        if domainConfig.classification:
            record.append(self.classification)        
        
        if domainConfig.resultScript:
            record.append(self.resultScript)        

        if domainConfig.ctree:
            record.append(self.depth)        
            record.append(self.immediateAncestor)            
            record.append(self.rootAncestor)
            record.append(self.descendantCount)

        if len(domainConfig.zones)>0:
            record.append(self.zone)        
       
        if domainConfig.coordinates:
            record.append(self.x)        
            record.append(self.y)

            if domainConfig.positionalAccuracy:
                record.append(self.positionalAccuracy)        

        
        record.append(self.visibility)
        
        record.append(self.security)
                
        if domainConfig.labelRanking:
            record.append(self.labelRanking)        
        
        return(tuple(record))

class SearchDomain():
    
    def __init__(self):   
        self.name = None
        self.configLocation=False
        self.highLowFilter=False
        self.zoneDefaultId = None
        self.zones=[]
        self.urn=False
        self.icon=False
        self.resultScript=False
        self.coordinates=False
        self.positionalAccuracy=False
        self.recordRanking=False
        self.labelRanking=False
        self.synopsis=False
        self.ctree=False
        self.ctreeDataType=None             
        self.ctreeDataTypeFull=None
        self.orderByStartingType = None
        self.orderByColumnCount = None        
        self.indexList = []
        
    def debug(self, settings):
        appLogger = settings.appLogger
        appLogger.debug("")
        appLogger.debug("Domain config")  
        appLogger.debug("-------------")
        appLogger.debug("  name                : %s" %(self.name))
        appLogger.debug("  highLowFilter       : %s" %(self.highLowFilter))
        appLogger.debug("  urn                 : %s" %(self.urn))
        appLogger.debug("  icon                : %s" %(self.icon))
        appLogger.debug("  coordinates         : %s" %(self.coordinates))
        appLogger.debug("  positionalAccuracy  : %s" %(self.positionalAccuracy))
        appLogger.debug("  recordRanking       : %s" %(self.recordRanking))
        appLogger.debug("  labelRanking        : %s" %(self.labelRanking))
        appLogger.debug("  orderByStartingType : %s" %(str(self.orderByStartingType)))
        appLogger.debug("  orderByColumnCount  : %s" %(str(self.orderByColumnCount)))
        appLogger.debug("  synopsis            : %s" %(str(self.synopsis)))
        appLogger.debug("  ctree               : %s (%s)" %(str(self.ctree), self.ctreeDataTypeFull))
        appLogger.debug("  classification      : %s" %(str(self.classification)))
        appLogger.debug("")
        
        if self.zoneDefaultId is not None:
            appLogger.debug("  zones:")
            appLogger.debug("    defaultId = %s" %(self.zoneDefaultId))
        for thisZone in self.zones:
            appLogger.debug("    %s" %(str(thisZone)))
        appLogger.debug("")

    def calculateZoneExtents(self, cursor):
        for thisZone in self.zones:
            thisZone.calculateExtents(cursor)
    
    def writeDomainFunctions(self, file, schemaName, databaseObjectType, databaseObjectName, searchConfig):
                
        def getScript(columnName):
            script =          "CREATE OR REPLACE FUNCTION search.get_best_%s_%s_label_for_%s (p_%s bigint) RETURNS character varying AS $$\n" %(self.name, databaseObjectName,columnName, columnName)
            script = script + "DECLARE\n"
            script = script + "  v_label character varying(500);\n"
         
            script = script + "BEGIN\n\n"
            
            # We don't know where it'll be...
            # (we could guess based on config, but the function might be overriden anyway!)
            
            if self.highLowFilter:
                script = script + "  -- high/low filter implementation for this domain, so check both tables\n"
                script = script + "  -- Note: Even though \"fixed\" attribute may be in use, it could be overridden\n\n"
                script = script + "  SELECT label\n"
                script = script + "  INTO v_label\n"
                script = script + "  FROM search.%s_high\n" %(self.name)                
                script = script + "  WHERE %s=p_%s\n" %(columnName,columnName)
                script = script + "  AND table_name='%s'" %(databaseObjectName)
                                
                if self.labelRanking:
                    script = script + "\n  ORDER BY label_ranking;\n\n"
                else:
                    script = script + ";\n\n"
                        
                script = script + "  IF v_label IS NULL THEN\n"
                script = script + "    SELECT label\n"
                script = script + "    INTO v_label\n"
                script = script + "    FROM search.%s_low\n" %(self.name)               
                script = script + "    WHERE %s=p_%s\n" %(columnName,columnName)
                script = script + "    AND table_name='%s'" %(databaseObjectName)
                
                if self.labelRanking:
                    script = script + "\n    ORDER BY label_ranking;\n"
                else:
                    script = script + ";\n"
                    
                script = script + "  END IF;\n\n"
    
            script = script + "  RETURN v_label;\n"
            script = script + "END;\n"
            script = script + "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n"
            
            return(script)

        file.write(getScript("record_id"))
        
        if self.urn:
            file.write(getScript("urn"))


        
                                                
    def setFromCapabilitiesTag(self, searchDomainName, capabilitiesTag):
        
        class Zone():

            def __init__(self, id, schema, table, column):
                self.id = id
                self.schema = schema
                self.table = table
                self.column = column
                self.xmin = None
                self.ymin = None
                self.xmax = None
                self.ymax = None
                self.withinSql = "select %s.is_point_within_%s(" %(self.schema, self.table)
                self.withinSql = self.withinSql + "%s,%s)"
                
            def calculateExtents(self,cursor):
                sql = "select ST_XMin(%s) as minx, ST_YMin(%s) as miny, ST_XMax(%s) As maxx, ST_YMax(%s) as maxy from %s.%s" %(self.column,self.column,self.column,self.column,self.schema,self.table)
                cursor.execute(sql)
                extents=cursor.fetchone()
                self.xmin = int(extents[0])
                self.ymin = int(extents[1])
                self.xmax = int(extents[2])
                self.ymax = int(extents[3])
                          
      
            def __str__(self):
                return ("%s: %s.%s(%s) [%s,%s]-[%s,%s]" %(self.id,self.schema, self.table, self.column,self.xmin, self.ymin, self.xmax, self.ymax))
            
        def isCapabilityEnabled(tagName):
            tag=capabilitiesTag.getElementsByTagName(tagName)
            if len(tag)>0:
                tag=tag[0]
                enabled = cs.grabAttribute(tag,"enabled")
                if enabled is None:
                    enabled=True
                else:
                    if enabled=="true":
                        enabled=True
                    else:
                        enabled=False
            else:
                enabled=False
                
            return(enabled)
                
        self.name = searchDomainName     
        self.highLowFilter = isCapabilityEnabled("highLowFilter")
        self.coordinates = isCapabilityEnabled("coordinates")
        self.positionalAccuracy = isCapabilityEnabled("positionalAccuracy")
        self.urn = isCapabilityEnabled("urn")
        self.icon = isCapabilityEnabled("icon")
        self.resultScript = isCapabilityEnabled("resultScript")
        self.recordRanking = isCapabilityEnabled("recordRanking")
        self.labelRanking = isCapabilityEnabled("labelRanking")
        self.synopsis = isCapabilityEnabled("synopsis")
        self.classification = isCapabilityEnabled("classification")
                
        self.ctree = isCapabilityEnabled("ctree")
        if self.ctree:
            tag = capabilitiesTag.getElementsByTagName("ctree")[0]
            self.ctreeDataType=cs.grabAttribute(tag,"dataType")
            
            if self.ctreeDataType =="character varying":
                size=int(cs.grabAttribute(tag,"size")) 
                self.ctreeDataTypeFull="%s(%d)" %(self.ctreeDataType, size)
            else:
                self.ctreeDataTypeFull=self.ctreeDataType                            
            

        orderByTag=capabilitiesTag.getElementsByTagName("orderBy")
        if len(orderByTag)>0:    
            orderByTag = orderByTag[0]
            self.orderByStartingType = cs.grabAttribute(orderByTag,"startingType")            
            self.orderByColumnCount = cs.grabAttribute(orderByTag,"columnCount")
            if self.orderByColumnCount is not None:
                self.orderByColumnCount = int(self.orderByColumnCount)
                    
        zonesTag=capabilitiesTag.getElementsByTagName("zones")
        if len(zonesTag)>0:
            zonesTag=zonesTag[0]                
            self.zoneDefaultId = cs.grabAttribute(zonesTag,"defaultId")
            
            zones=zonesTag.getElementsByTagName("zone")
            for thisZone in zones:
                id = int(cs.grabAttribute(thisZone,"id"))
                schema = cs.grabAttribute(thisZone,"schema")
                table = cs.grabAttribute(thisZone,"table")
                column = cs.grabAttribute(thisZone,"column")
                self.zones.append(Zone(id,schema,table,column))
        
        
        self.forbiddenSearches=[]
        forbiddenSearchesTag=capabilitiesTag.getElementsByTagName("forbiddenSearches")
        if len(forbiddenSearchesTag)>0:
            forbiddenSearchesTag = forbiddenSearchesTag[0]
            searchTags=forbiddenSearchesTag.getElementsByTagName("search")
            for thisSearch in searchTags:
                text = cs.grabAttribute(thisSearch,"text")
                text = text.lower()
                text = text.strip()
                self.forbiddenSearches.append(text)
                                                
    def setFromFile(self, settings, searchDomainName):
        filename = os.path.join(settings.paths["repository"], "search domains", "%s.xml" %(searchDomainName))                
        xmldoc = xml.dom.minidom.parse(filename)
        capabilitiesTag = xmldoc.getElementsByTagName("searchCapabilities")[0]    
        self.setFromCapabilitiesTag(searchDomainName,capabilitiesTag)  
        xmldoc.unlink()

    def registerIndex(self, table, indexName, ddl, schema):
        self.indexList.append((table, indexName,ddl.strip("\n"), schema))

  
  
    def getPostgreSQLBuildScript(self, configLocation, settings, appLogger):
 
        srid = int(settings.env["srid"])
 
        def getTableScript(suffix):
            table = "CREATE TABLE search."
            table_name=self.name
            if suffix is not None:
                table_name=table_name + "_%s" %(suffix)
            
            table=table + "%s (\n" %(table_name)
            
            table=table + "  record_id bigint NOT NULL,\n"
            table=table + "  table_name character varying(200) NOT NULL,\n"
            
            if self.recordRanking:
                table=table + "  fixed_ranking_offset smallint NOT NULL,\n"
                
            table=table + "  label character varying(500) NOT NULL,\n"
            table=table + "  search character varying(500) NOT NULL,\n"
            table=table + "  search_vector tsvector NOT NULL,\n"

            if self.urn:
                table=table + "  urn bigint,\n"

            if self.icon:
                table=table + "  icon character varying (60),\n"
            
            if self.synopsis:
                table=table + "  synopsis character varying (4000),\n"
    
            if self.classification:
                table=table + "  classification character varying (30),\n"
                
            if self.resultScript:
                table=table + "  result_script character varying (4000),\n"
                
            if self.ctree:
                table=table + "  depth integer,\n"
                table=table + "  immediate_ancestor %s,\n" %(self.ctreeDataTypeFull)
                table=table + "  root_ancestor %s,\n" %(self.ctreeDataTypeFull)
                table=table + "  descendant_count integer,\n"
                                
            if len(self.zones)>0:
                table=table + "  zone integer NOT NULL,\n"
                
            if self.coordinates:
                table=table + "  x numeric(9,2),\n"
                table=table + "  y numeric(9,2),\n"
                table=table + "  pin geometry,\n"
                
                if self.positionalAccuracy:
                    table=table + "  positional_accuracy integer NOT NULL,\n"
                
            table=table + "  visibility smallint NOT NULL,\n"
            table=table + "  security smallint NOT NULL,\n"
            
            if self.labelRanking:
                table=table + "  label_ranking smallint NOT NULL,\n"
                      
            if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                thisType=self.orderByStartingType
                numberIdx = 0
                textIdx=0
                for i in range(0,self.orderByColumnCount):
                    if thisType=='number':
                        numberIdx=numberIdx+1
                        table=table + "  order_by_number%d integer NOT NULL,\n" %(numberIdx)
                        if i==0:
                            orderByDelimited="order_by_number%d"%(numberIdx)
                        else:
                            orderByDelimited=orderByDelimited+", order_by_number%d"%(numberIdx)
                        thisType = "text"
                    
                    else:
                        textIdx=textIdx+1
                        table=table + "  order_by_char%d character varying (100) NOT NULL,\n" %(textIdx)
                        if i==0:
                            orderByDelimited="order_by_char%d"%(textIdx)
                        else:
                            orderByDelimited=orderByDelimited+", order_by_char%d"%(textIdx)
                        thisType = "number"
            
            table=table + "  modified timestamp with time zone NOT NULL DEFAULT current_timestamp,\n"
            table=table + "  PRIMARY KEY (record_id, table_name));\n\n"
                        
            
            indexDdl="CREATE INDEX %s_search_vector ON search.%s USING gin(search_vector);\n\n" %(table_name, table_name)
            self.registerIndex("search."+table_name, "%s_search_vector"%(table_name), indexDdl, "search")
            table=table + indexDdl

            if self.urn:
                indexDdl="CREATE INDEX %s_search_urn ON search.%s (urn);\n\n" %(table_name, table_name)
                self.registerIndex("search."+table_name, "%s_search_urn"%(table_name), indexDdl, "search")
                table=table + indexDdl


            if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                indexDdl="CREATE INDEX %s_order_by ON search.%s (fixed_ranking_offset, %s);\n\n" %(table_name, table_name, orderByDelimited)
                self.registerIndex("search."+table_name, "%s_order_by"%(table_name), indexDdl, "search")
                table=table + indexDdl
 
            if self.coordinates:
                indexDdl="CREATE INDEX %s_search_pin_idx ON search.%s USING gist(pin);\n\n" %(table_name, table_name)
                self.registerIndex("search."+table_name, "%s_search_pin_idx"%(table_name), indexDdl, "search")
                table=table + indexDdl
            
            if self.ctree:
                indexDdl="CREATE INDEX %s_depth ON search.%s (depth);\n\n" %(table_name, table_name)
                self.registerIndex("search."+table_name, "%s_depth"%(table_name), indexDdl, "search")
                table=table + indexDdl

                indexDdl="CREATE INDEX %s_immediate_ancestor ON search.%s (immediate_ancestor);\n\n" %(table_name, table_name)
                self.registerIndex("search."+table_name, "%s_immediate_ancestor"%(table_name), indexDdl, "search")
                table=table + indexDdl

                indexDdl="CREATE INDEX %s_root_ancestor ON search.%s (root_ancestor);\n\n" %(table_name, table_name)
                self.registerIndex("search."+table_name, "%s_root_ancestor"%(table_name), indexDdl, "search")
                table=table + indexDdl
                                                    
            return(table)

        def getOrderByFunctionScript():
            func =        "CREATE OR REPLACE FUNCTION search.get_%s_order_by(p_text character varying)\n" %(self.name)
            func = func + "  RETURNS CHARACTER VARYING[] AS $$\n"
            func = func + "DECLARE\n"
            func = func + "  v_buffer CHARACTER VARYING(500);\n"
            func = func + "  v_mode integer;\n"
            func = func + "  v_start_type CHARACTER VARYING(10)='%s';\n" %(self.orderByStartingType)
            
            templateString=''
            if self.orderByStartingType =='number':
                templateColumnType='99999';
            else:
                templateColumnType='ZZZZZ';
            
            for i in range(0,self.orderByColumnCount):
                if i>0:
                    templateString = templateString + ','
                templateString = templateString + '\'%s\'' %(templateColumnType)
                if templateColumnType=='99999':
                    templateColumnType = 'ZZZZZ'
                else:
                    templateColumnType = '99999'
            
            func = func + "  v_dest_array CHARACTER VARYING(100)[] = ARRAY[%s];\n" %(templateString)
            func = func + "  v_column_array_size INTEGER = %d;\n" %(self.orderByColumnCount)
            func = func + "  v_dest_index INTEGER;\n"
            func = func + "BEGIN\n"
            func = func + "  IF p_text IS NOT NULL THEN\n"
            func = func + "    IF LENGTH(p_text) >0 THEN\n"  
            func = func + "      v_buffer = '';\n"
            func = func + "      IF substr(p_text,1,1) IN('0','1','2','3','4','5','6','7','8','9') THEN\n"
            func = func + "        v_mode= 1;\n"
            
            if self.orderByStartingType=='number':
                func = func + "        v_dest_index = 1;\n"
            else:
                func = func + "        v_dest_index = 2;\n"
            
            func = func + "      ELSE\n"

            func = func + "        v_mode= 2;\n"
            if self.orderByStartingType=='text':
                func = func + "        v_dest_index = 1;\n"
            else:
                func = func + "        v_dest_index = 2;\n"            
            
            func = func + "      END IF;\n"
            func = func + "      FOR i IN 1..length(p_text) LOOP\n"
            func = func + "        IF v_mode=1 THEN\n"       
            func = func + "          IF substr(p_text,i,1) IN('0','1','2','3','4','5','6','7','8','9') THEN\n"
            func = func + "            v_buffer=v_buffer||substr(p_text,i,1);\n"
            func = func + "          ELSE\n"
            func = func + "            v_mode=2;\n"
            func = func + "            v_buffer = trim(v_buffer);\n"
            func = func + "            IF v_buffer != '' AND v_dest_index <= v_column_array_size THEN\n"
            func = func + "              v_dest_array[v_dest_index]=substr(v_buffer,1,8);\n"
            func = func + "            END IF;\n"
            func = func + "            v_dest_index=v_dest_index+1;\n"
            func = func + "            v_buffer = substr(p_text,i,1);\n"            
            func = func + "          END IF;\n"            
            func = func + "        ELSE\n"        
            func = func + "          IF substr(p_text,i,1) IN('0','1','2','3','4','5','6','7','8','9') THEN\n"
            func = func + "            v_mode=1;\n"
            func = func + "            v_buffer = trim(v_buffer);\n"
            func = func + "            IF v_buffer !='' AND v_dest_index <= v_column_array_size THEN\n"
            func = func + "              v_dest_array[v_dest_index]=substr(v_buffer,1,100);\n"
            func = func + "            END IF;\n"
            func = func + "            v_dest_index=v_dest_index+1;\n"
            func = func + "            v_buffer = substr(p_text,i,1);\n"          
            func = func + "          ELSE\n"
            func = func + "            v_buffer=v_buffer||substr(p_text,i,1);\n"
            func = func + "          END IF;\n"                
            func = func + "        END IF;\n"              
            func = func + "      END LOOP;\n"
            func = func + "      v_buffer = trim(v_buffer);\n"       
            func = func + "      IF v_buffer != '' AND v_dest_index <= v_column_array_size THEN\n"
            func = func + "        IF v_mode=1 THEN\n"
            func = func + "          v_dest_array[v_dest_index]=substr(v_buffer,1,8);\n"
            func = func + "        ELSE\n"     
            func = func + "          v_dest_array[v_dest_index]=substr(v_buffer,1,100);\n"
            func = func + "        END IF;\n"
            func = func + "      END IF;\n"
            func = func + "    END IF;\n"
            func = func + "  END IF;\n"
            func = func + "  RETURN v_dest_array;\n"
            func = func + "END;\n"
            func = func + "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n"

            return(func)

        def getTriggerFunctionScript():

                trigger =         "CREATE OR REPLACE FUNCTION search.%s_insert_processor()\n" %(self.name)
                trigger=trigger + "  RETURNS trigger AS $$\n"
                trigger=trigger + "DECLARE\n"
                
                if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                    trigger=trigger + "  v_array CHARACTER VARYING(100)[];\n"
                
                trigger=trigger + "BEGIN\n"
                trigger=trigger + "  new.search = shared.simple_text(new.search);\n"
                trigger=trigger + "  new.search_vector = to_tsvector(new.search);\n"

                if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                    trigger=trigger + "  v_array = search.get_%s_order_by(new.search);\n" %(self.name)
                    columnType = self.orderByStartingType
                    numberIdx=0
                    textIdx=0
                    for i in range(0,self.orderByColumnCount):
                        if columnType=='number':
                            numberIdx=numberIdx+1
                            trigger=trigger + "  new.order_by_number%d = v_array[%d]::integer;\n" %(numberIdx,i+1)
                            columnType='text'
                        else:
                            textIdx=textIdx+1
                            trigger=trigger + "  new.order_by_char%d = v_array[%d];\n" %(numberIdx,i+1)
                            columnType='number'

                if self.coordinates:
                    trigger=trigger + "  new.pin = ST_GeometryFromText('POINT('||new.x::character varying||' '||new.y::character varying||')',27700);\n"
                    
                    
                trigger=trigger + "  RETURN new;\n"
                trigger=trigger + "END;\n"
                trigger=trigger + "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n"

                trigger=trigger + "CREATE OR REPLACE FUNCTION search.%s_update_processor()\n" %(self.name)
                trigger=trigger + "  RETURNS trigger AS $$\n"
                trigger=trigger + "DECLARE\n"
                trigger=trigger + "  v_simple_search character varying(500);\n"
                
                if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                    trigger=trigger + "  v_array CHARACTER VARYING(100)[];\n"

                trigger=trigger + "BEGIN\n"
                trigger=trigger + "  v_simple_search = shared.simple_text(new.search);\n"
                trigger=trigger + "  IF shared.different(old.search, v_simple_search) THEN\n"
                trigger=trigger + "    new.search = v_simple_search;\n"
                trigger=trigger + "    new.search_vector = to_tsvector(new.search);\n"
                
                if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                    trigger=trigger + "    v_array = search.get_%s_order_by(new.search);\n" %(self.name)
                    columnType = self.orderByStartingType
                    numberIdx=0
                    textIdx=0
                    for i in range(0,self.orderByColumnCount):
                        if columnType=='number':
                            numberIdx=numberIdx+1
                            trigger=trigger + "    new.order_by_number%d = v_array[%d]::integer;\n" %(numberIdx,i+1)
                            columnType='text'
                        else:
                            textIdx=textIdx+1
                            trigger=trigger + "    new.order_by_char%d = v_array[%d];\n" %(numberIdx,i+1)
                            columnType='number'
                    
                trigger=trigger + "  END IF;\n"

                if self.coordinates:
                    trigger=trigger + "\n  IF shared.different(old.x, new.x) OR\n"
                    trigger=trigger + "     shared.different(old.y, new.y) THEN\n"
                    trigger=trigger + "    new.pin = ST_GeometryFromText('POINT('||new.x::character varying||' '||new.y::character varying||')',27700);\n"
                    trigger=trigger + "  END IF;\n\n"

                
                trigger=trigger + "  RETURN new;\n"
                trigger=trigger + "END;\n"
                trigger=trigger + "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n"

                return(trigger)

        def getWithinZoneScript(settings):
            def getScript(dataType):
                
                script=         "CREATE OR REPLACE FUNCTION %s.is_point_within_%s(p_x %s, p_y %s)\n" %(thisZone.schema, thisZone.table, dataType, dataType)
                script=script + "  RETURNS boolean AS $$\n"
                script=script + "DECLARE\n"
                script=script + "  v_within boolean;\n"
                script=script + "BEGIN\n"
                script=script + "  SELECT ST_Within(ST_GeomFromText('POINT('||p_x||' '||p_y||')',%s),%s)\n" %(srid,thisZone.column)
                script=script + "  INTO v_within\n"
                script=script + "  FROM %s.%s;\n" %(thisZone.schema, thisZone.table)
                script=script + "  RETURN v_within;\n"
                script=script + "END;\n"
                script=script + "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n"
                
                return(script)

            
            fullScript=""
            for thisZone in self.zones:
                fullScript=fullScript+getScript("numeric")
                fullScript=fullScript+getScript("float")
            
            return(fullScript)

#select id,x,y,fid,visibility,security FROM pinhead.ows WHERE ST_Within(pin, $1) ' USING p_geometry LOOP
                
            return(script)
            
        def getTriggerScript(suffix):
            trigger = "CREATE TRIGGER "
            tableName=self.name
            if suffix is not None:
                tableName=tableName + "_%s" %(suffix)
            
            trigger=trigger + "%s_insert\n" %(tableName)
            trigger=trigger + "  BEFORE INSERT\n"
            trigger=trigger + "  ON search.%s\n" %(tableName)
            trigger=trigger + "  FOR EACH ROW\n"
            trigger=trigger + "  EXECUTE PROCEDURE search.%s_insert_processor();\n\n"  %(self.name)

            trigger=trigger + "CREATE TRIGGER "            
            trigger=trigger + "%s_update\n" %(tableName)
            trigger=trigger + "  BEFORE UPDATE\n"
            trigger=trigger + "  ON search.%s\n" %(tableName)
            trigger=trigger + "  FOR EACH ROW\n"
            trigger=trigger + "  EXECUTE PROCEDURE search.%s_update_processor();\n\n" %(self.name)
            
            return(trigger)

        def getDataExistsScript():                
            script =        "CREATE OR REPLACE FUNCTION search.is_there_any_%s_data()\n" %(self.name)
            script = script+"  RETURNS boolean AS\n"
            script = script+"$$\n"
            script = script+"  DECLARE\n"
            
            if self.highLowFilter:
                script = script+"    v_high_result boolean;\n"
                script = script+"    v_low_result boolean;\n"
            else:
                script = script+"    v_result boolean;\n"
                
            script = script+"  BEGIN\n"
            
            if self.highLowFilter:
                script = script+"    SELECT exists(select 1 from search.%s_low limit 1) INTO v_low_result;\n" %(self.name)
                script = script+"    SELECT exists(select 1 from search.%s_high limit 1) INTO v_high_result;\n" %(self.name)
                script = script+"    RETURN (v_low_result or v_high_result);\n"
            else:
                script = script+"    SELECT exists(select 1 from search.%s limit 1) INTO v_result;\n" %(self.name)
                script = script+"    RETURN (v_result);\n"
            
            
            script = script+"  END;\n"
            script = script+"$$\n"
            script = script+"  LANGUAGE 'plpgsql';\n\n"            
            return(script)


        def getSearchFunctionsScript():
            
            def getDomainSearchResultType():
                script = "CREATE TYPE search.%s_result AS (\n" %(self.name)
                script = script + "  record_id bigint,\n"
                script = script + "  table_name character varying(200),\n"
                
                if self.urn:
                    script = script + "  urn bigint,\n"
                if self.icon:
                    script = script + "  icon character varying(60),\n"
                
                script = script + "  label character varying(500),\n"
                if self.synopsis:
                    script = script + "  synopsis character varying(4000),\n"
                
                if self.classification:
                    script = script + "  classification character varying(30),\n"
                
                if self.resultScript:
                    script = script + "  result_script character varying(4000),\n"

                if self.ctree:
                    script = script + "  depth integer,\n"
                    script = script + "  immediate_ancestor %s,\n" %(self.ctreeDataTypeFull)
                    script = script + "  root_ancestor %s,\n" %(self.ctreeDataTypeFull)
                    script = script + "  descendant_count integer,\n"
                
                if len(self.zones)>0:
                    script = script + "  zone integer,\n"
                
                if self.coordinates:
                    script = script + "  x numeric,\n"
                    script = script + "  y numeric,\n"
                    script = script + "  distance numeric,\n"
                    
                    if self.positionalAccuracy:
                        script = script + "  positional_accuracy integer,\n"

                
                script = script + "  visibility smallint,\n"
                script = script + "  security smallint,\n"
                script = script + "  modified timestamp with time zone);\n\n"
                                            
                return script;



            def getDomainSearchParametersType():
                script = "CREATE TYPE search.%s_parameters AS (\n" %(self.name)
                script = script + "  search_text tsquery,\n"

                if len(self.zones)>0:
                    script = script + "  min_zone integer,\n"
                    script = script + "  max_zone integer,\n"

                if self.ctree:
                    script = script + "  min_depth integer,\n"
                    script = script + "  max_depth integer,\n"
                    script = script + "  ancestor %s,\n" %(self.ctreeDataTypeFull)
                    
                if self.coordinates:
                    script = script + "  anchor_x numeric(9,2),\n"
                    script = script + "  anchor_y numeric(9,2),\n"
                    script = script + "  area geometry,\n"
                    
                if self.urn:
                    script = script + "  urn_search bigint,\n"
                
                script = script + "  min_visibility integer,\n"
                script = script + "  max_visibility integer,\n"
                script = script + "  min_security integer,\n"
                script = script + "  max_security integer,\n"
                script = script + "  order_by character varying,\n"
                script = script + "  row_limit integer);\n\n"
                                                            
                return script;

            def getParamList(includeGeometryParameter, additionalSpatialParameters):
                sig = "p_search_text character varying"
                
                if self.highLowFilter:
                    sig=sig+",p_filter_name character varying"
                
                if self.urn:
                    sig=sig+",p_search_urn boolean"
                
                if len(self.zones)>0:
                    sig=sig+",p_min_zone integer,p_max_zone integer"

                if self.ctree:
                    sig=sig+",p_min_depth integer,p_max_depth integer,p_ancestor %s" %(self.ctreeDataType)
                    
                if self.coordinates:
                    sig=sig+",p_anchor_x numeric,p_anchor_y numeric"
                    if includeGeometryParameter:
                        sig=sig+",p_area geometry"
                    if additionalSpatialParameters is not None:
                        sig=sig+","+additionalSpatialParameters
                    
                sig=sig+",p_min_visibility integer,p_max_visibility integer,p_min_security integer,p_max_security integer"
                sig=sig+",p_order_by character varying,p_row_limit integer,p_table_restriction character varying"
                sig=sig+",p_application_name character varying,p_username character varying"

                return(sig)

#SELECT record_id,table_name,urn,icon,label,synopsis,classification,result_script, zone, x, y,positional_accuracy, visibility, security, modified';
            def getSelectList(includeDistance): 

                distanceIdx = None
                               
                select = "record_id,table_name"
                idx = 2
                if self.urn:
                    select = select + ",urn"
                    idx=idx+1
                    
                if self.icon:
                    select = select + ",icon"
                    idx=idx+1
                
                select = select + ",label"
                idx=idx+1
                
                if self.synopsis:
                    select = select + ",synopsis"
                    idx=idx+1
                
                if self.classification:
                    select = select + ",classification"
                    idx=idx+1
                
                if self.resultScript:
                    select = select + ",result_script"
                    idx=idx+1


                if self.ctree:
                    select = select + ",depth,immediate_ancestor,root_ancestor,descendant_count"
                    idx=idx+4
                
                if len(self.zones)>0:
                    select = select + ",zone"
                    idx=idx+1
                
                if self.coordinates:
                    select = select + ",x"
                    select = select + ",y"
                    if includeDistance:
                        select = select + ",shared.get_distance(x,y,$1.anchor_x,$1.anchor_y) AS distance"
                    else:
                        select = select + ",null::numeric"
                    idx=idx+3
                    distanceIdx = idx
                    
                    if self.positionalAccuracy:
                        select = select + ",positional_accuracy"
                        idx=idx+1
                                
                select = select + ",visibility"
                select = select + ",security"
                select = select + ",modified"
                idx=idx+3

                return((select,distanceIdx))
                        
            def getRankedOrderBy():
                if self.orderByStartingType is not None and self.orderByColumnCount is not None:
                    orderByDelimited="fixed_ranking_offset"
                    thisType=self.orderByStartingType
                    numberIdx = 0
                    textIdx=0
                    for i in range(0,self.orderByColumnCount):
                        if thisType=='number':
                            numberIdx=numberIdx+1
                            orderByDelimited=orderByDelimited+", order_by_number%d"%(numberIdx)
                            thisType = "text"
                        
                        else:
                            textIdx=textIdx+1
                            orderByDelimited=orderByDelimited+", order_by_char%d"%(textIdx)
                            thisType = "number"
                else:
                    orderByDelimited="fixed_ranking,label"
                    
                return(orderByDelimited)


            def getTextFilter(tableSuffix):
                
                if tableSuffix is None:
                    functionName='%s_text_filter'  %(self.name)                    
                else:
                    functionName='%s_%s_text_filter'  %(self.name, tableSuffix)
                    
                script =          "CREATE OR REPLACE FUNCTION search.%s(p_query tsquery" %(functionName)
                
                if self.urn:
                    script = script + ", p_urn_search bigint"
                
                script = script + ")\n" 
                
                
                if tableSuffix is None:
                    returnType = self.name
                else:
                    returnType = '%s_%s' %(self.name,tableSuffix)
                    
                
                script = script + "RETURNS SETOF search.%s AS\n" %(returnType)
                script = script + "$BODY$\n"
                script = script + "  BEGIN\n"
                
                if self.urn:
                    script = script + "    IF p_urn_search IS NULL THEN\n"
                    
                
                script = script + "      RETURN QUERY\n"
                script = script + "        SELECT *\n" 

                if tableSuffix is None:                    
                    script = script + "        FROM search.%s\n" %(self.name)
                else:
                    script = script + "        FROM search.%s_%s\n" %(self.name, tableSuffix)
               
                script = script + "        WHERE search_vector @@ p_query;\n" 
                
                                 
                if self.urn:
                    script = script + "    ELSE\n"
                    script = script + "      RETURN QUERY\n"
                    script = script + "        SELECT *\n" 
    
                    if tableSuffix is None:                    
                        script = script + "        FROM search.%s\n" %(self.name)
                    else:
                        script = script + "        FROM search.%s_%s\n" %(self.name, tableSuffix)
                   
                    script = script + "        WHERE (search_vector @@ p_query or urn=p_urn_search);\n" 
                    script = script + "    END IF;\n"
                    
                                 
                script = script + "  END;\n"
                script = script + "$BODY$\n"
                script = script + "  LANGUAGE 'plpgsql';\n\n"

                return (script)


            def getAreaFilter(tableSuffix):
                
                if tableSuffix is None:
                    functionName='%s_area_filter'  %(self.name)                    
                else:
                    functionName='%s_%s_area_filter'  %(self.name, tableSuffix)
                    
                script =          "CREATE OR REPLACE FUNCTION search.%s(p_area geometry)\n" %(functionName)

                if tableSuffix is None:
                    returnType = self.name
                else:
                    returnType = '%s_%s' %(self.name,tableSuffix)

                
                script = script + "RETURNS SETOF search.%s AS\n" %(returnType)
                script = script + "$BODY$\n"
                script = script + "  BEGIN\n"
                script = script + "    RETURN QUERY\n"
                script = script + "      SELECT *\n" 

                if tableSuffix is None:                    
                    script = script + "      FROM search.%s\n" %(self.name)
                else:
                    script = script + "      FROM search.%s_%s\n" %(self.name, tableSuffix)
               
                script = script + "      WHERE ST_Within(pin,p_area);\n" 
                                 
                script = script + "  END;\n"
                script = script + "$BODY$\n"
                script = script + "  LANGUAGE 'plpgsql';\n\n"

                return (script)


            def getAncestorFilter(tableSuffix):
                
                if tableSuffix is None:
                    functionName='%s_ancestor_filter'  %(self.name)                    
                else:
                    functionName='%s_%s_ancestor_filter'  %(self.name, tableSuffix)
                    
                script =          "CREATE OR REPLACE FUNCTION search.%s(p_ancestor_urn bigint)\n" %(functionName)
                
                if tableSuffix is None:
                    returnType = self.name
                else:
                    returnType = '%s_%s' %(self.name,tableSuffix)
                
                script = script + "RETURNS SETOF search.%s AS\n" %(returnType)
                script = script + "$BODY$\n"
                script = script + "  BEGIN\n"
                script = script + "    RETURN QUERY\n"
                script = script + "      SELECT *\n" 

                if tableSuffix is None:                    
                    script = script + "      FROM search.%s\n" %(self.name)
                else:
                    script = script + "      FROM search.%s_%s\n" %(self.name, tableSuffix)
               
                script = script + "      WHERE root_ancestor_urn=p_ancestor_urn;\n" 
                                 
                script = script + "  END;\n"
                script = script + "$BODY$\n"
                script = script + "  LANGUAGE 'plpgsql';\n\n"

                return (script)

             
               
            def getDomainSearchCore():

                paramList = getParamList(True, None)

                script =         "CREATE OR REPLACE FUNCTION search.get_best_%s_filter(p_text tsquery, p_area geometry, p_ancestor_urn %s) returns character varying AS\n" %(self.name, self.ctreeDataType)
                script = script +"$BODY$\n"
                script = script +"  DECLARE\n"
                script = script +"    r character varying (30);\n"
                script = script +"  BEGIN\n"
                script = script +"    IF p_area IS NOT NULL THEN\n"
                script = script +"      r = 'area';\n"
                script = script +"    ELSE\n"
                script = script +"      IF p_text IS NOT NULL THEN\n"
                script = script +"        r = 'text';\n"
                script = script +"      ELSE\n"
                script = script +"        IF p_ancestor_urn IS NOT NULL THEN\n"
                script = script +"          r = 'ancestor';\n"
                script = script +"        END IF;\n"
                script = script +"      END IF;\n"
                script = script +"    END IF;\n"
                script = script +"    RETURN(r);\n"
                script = script +"  END;\n"
                script = script +"$BODY$\n"
                script = script +"LANGUAGE 'plpgsql';\n\n"
                

                script = script +"CREATE TABLE search.%s_queue (\n" %(self.name)
                script = script +"  record_id bigint NOT NULL,\n"
                script = script +"  table_name character varying(200),\n"
                script = script +"  PRIMARY KEY (record_id, table_name));\n\n"
                
                
                
                script = script +"CREATE TABLE search.forbidden_%s_searches (\n" %(self.name)
                script = script +"  search_text CHARACTER VARYING(200) PRIMARY KEY);\n\n"
                
                for thisForbiddenSearch in self.forbiddenSearches:
                    script = script +"INSERT INTO search.forbidden_%s_searches (search_text) VALUES ('%s');\n" %(self.name, thisForbiddenSearch)
                
                script = script + "\nCREATE OR REPLACE FUNCTION working.search_%s(%s)\n" %(self.name,paramList)
                script = script + "RETURNS SETOF search.%s_result AS\n" %(self.name)
                script = script + "$BODY$\n"
                script = script + "  DECLARE\n\n"

                script = script + "    v_parameters search.%s_parameters" %(self.name) + "%rowtype;\n"
                script = script + "    v_result search.%s_result" %(self.name) + "%rowtype;\n\n"

                script = script + "    v_select_clause character varying(2000);\n"
                script = script + "    v_from_clause character varying(500);\n"
                script = script + "    v_best_filter character varying(10);\n"
                    
                script = script + "    v_where_conditions character varying(500)[];\n"                
                script = script + "    v_order_by_clause character varying(500);\n\n"
    
                script = script + "    v_refined_search_text character varying(200);\n"
                script = script + "    v_phrase character varying(200);\n"
                script = script + "    v_refined character varying(200);\n"
                script = script + "    v_row_count integer;\n"                
                script = script + "    v_ignore integer;\n"
                
                script = script + "    v_sql character varying(2000);\n"
                if self.highLowFilter:
                    script = script + "    v_perform_low_level_search boolean;\n"#

                script = script + "  BEGIN\n\n"

                script = script + "    -- Process text\n"
                script = script + "    v_refined_search_text = lower(ltrim(rtrim(p_search_text)));\n"
                
                script = script + "    SELECT 1\n"
                script = script + "    INTO v_ignore\n"
                script = script + "    FROM search.forbidden_%s_searches\n" %(self.name)
                script = script + "    WHERE search_text = v_refined_search_text;\n"
                
                script = script + "    IF FOUND THEN\n"
                script = script + "      RAISE EXCEPTION 'CHIMP-TOOVAGUE' USING HINT='Your search is too vague and will return a large amount of superfluous results. Re-specify your search to make it more specific.';\n"
                script = script + "    END IF;\n"                            
                script = script + "    IF v_refined_search_text IS NOT NULL THEN\n"
                script = script + "      v_parameters.search_text = plainto_tsquery(v_refined_search_text);\n"
                script = script + "    END IF;\n\n"


                script = script + "    -- SELECT CLAUSE\n"
                script = script + "    -- =============\n\n"
                
                (selectList,distanceIdx) = getSelectList(True)      
                script = script + "    v_select_clause = '%s';\n\n" %(selectList)
                
                script = script + "    -- FROM CLAUSE\n"
                script = script + "    -- ===========\n\n"
                                
                if self.coordinates:
                    script = script + "    -- Process area\n"
                    script = script + "    IF p_area IS NOT NULL THEN\n"
                    script = script + "      v_parameters.area = p_area;\n"                
                    script = script + "    END IF;\n\n"
                    areaValue="v_parameters.area"
                else:
                    areaValue="null"
                
                if self.ctree:
                    script = script + "    -- Process ancestor\n"
                    script = script + "    IF p_ancestor IS NOT NULL THEN\n"
                    script = script + "      v_parameters.ancestor = p_ancestor;\n"                
                    script = script + "    END IF;\n\n"
                    ancestorValue="v_parameters.ancestor"
                else:
                    ancestorValue="null"
                

                
                script = script + "    v_best_filter = search.get_best_%s_filter(v_parameters.search_text,%s,%s);\n\n" %(self.name, areaValue, ancestorValue)
                
                script = script + "    IF v_best_filter IS NOT NULL THEN\n\n"
                
                # TEXT FILTER LED
                script = script + "      IF v_best_filter = 'text' THEN\n"
                                
                if self.urn:
                    script = script + "        IF shared.is_integer(v_refined_search_text) THEN\n"
                    script = script + "          v_parameters.urn_search = v_refined_search_text::bigint;\n"
                    script = script + "        END IF;\n"
                                
                clauseLine = "        v_from_clause   = 'search.%s"%(self.name)
                if self.highLowFilter:  
                    clauseLine = clauseLine +"_' || p_filter_name || '"                
                clauseLine=clauseLine + "_text_filter($1.search_text"
                if self.urn:
                    clauseLine=clauseLine+", $1.urn_search"
                clauseLine=clauseLine + ")';\n"
                script = script +clauseLine
                script = script + "      END IF;\n\n"

                # AREA FILTER LED
                if self.coordinates:
                    script = script + "      IF v_best_filter = 'area' THEN\n"                
                    clauseLine = "        v_from_clause   = 'search.%s"%(self.name)
                    if self.highLowFilter:  
                        clauseLine = clauseLine +"_' || p_filter_name || '"                
                    clauseLine=clauseLine + "_area_filter($1.area)';\n"
                    script = script +clauseLine
                    script = script + "      END IF;\n\n"

                # ANCESTOR FILTER LED
                if self.ctree:
                    script = script + "      IF v_best_filter = 'ancestor' THEN\n"                
                    clauseLine = "        v_from_clause   = 'search.%s"%(self.name)
                    if self.highLowFilter:  
                        clauseLine = clauseLine +"_' || p_filter_name || '"                
                    clauseLine=clauseLine + "_ancestor_filter($1.ancestor)';\n"
                    script = script +clauseLine
                    script = script + "      END IF;\n\n"
                
                script = script + "      -- WHERE CLAUSE\n"
                script = script + "      -- ============\n\n"
                                           
                
                script = script + "      IF v_best_filter != 'text' AND v_parameters.search_text IS NOT NULL THEN\n"
                script = script + "        v_where_conditions = ARRAY_APPEND(v_where_conditions, 'search_vector @@ $1.search_text');\n"                
                script = script + "      END IF;\n\n"

                if self.coordinates:
                    script = script + "      IF v_best_filter != 'area' AND p_area IS NOT NULL THEN\n"
                    script = script + "        v_where_conditions = ARRAY_APPEND(v_where_conditions, 'ST_Within(pin,$1.area)');\n"
                    script = script + "      END IF;\n\n"

                if self.ctree:
                    script = script + "      IF v_best_filter != 'ancestor' AND p_ancestor IS NOT NULL THEN\n"
                    script = script + "        v_where_conditions = ARRAY_APPEND(v_where_conditions, 'root_ancestor=$1.ancestor');\n"
                    script = script + "      END IF;\n\n"
                
                if len(self.zones)>0:                
                    script = script + "      -- Process zone restriction\n"
                    script = script + "      IF p_min_zone IS NOT NULL OR p_max_zone IS NOT NULL THEN\n"
                    script = script + "        v_parameters.min_zone = p_min_zone;\n"
                    script = script + "        v_parameters.max_zone = p_max_zone;\n"
                    script = script + "        IF p_min_zone IS NOT NULL AND p_max_zone IS NOT NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'zone BETWEEN $1.min_zone AND $1.max_zone');\n"
                    script = script + "        ELSIF p_min_zone IS NOT NULL AND p_max_zone IS NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'zone >= $1.min_zone');\n"
                    script = script + "        ELSIF p_min_zone IS NULL AND p_max_zone IS NOT NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'zone <= $1.max_zone');\n"
                    script = script + "        END IF;\n"
                    script = script + "      END IF;\n\n"

                if self.coordinates:
                    script = script + "      -- Process Anchor (by adding a distance value)\n"
                    script = script + "      IF p_anchor_x IS NOT NULL AND p_anchor_y IS NOT NULL THEN\n"
                    script = script + "        v_parameters.anchor_x = p_anchor_x;\n"
                    script = script + "        v_parameters.anchor_y = p_anchor_y;\n"    
                    script = script + "      END IF;\n\n"

                if self.ctree:
                    script = script + "      -- Process depth restriction\n"
                    script = script + "      IF p_min_depth IS NOT NULL OR p_max_depth IS NOT NULL THEN\n"
                    script = script + "        v_parameters.min_depth = p_min_depth;\n"
                    script = script + "        v_parameters.max_depth = p_max_depth;\n"
                    script = script + "        IF v_parameters.min_depth IS NOT NULL AND v_parameters.max_depth IS NOT NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'depth BETWEEN $1.min_depth AND $1.max_depth');\n"
                    script = script + "        ELSIF v_parameters.min_depth IS NOT NULL AND v_parameters.max_depth IS NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'depth >= $1.min_depth');\n"
                    script = script + "        ELSIF v_parameters.min_depth IS NULL AND v_parameters.max_depth IS NOT NULL THEN\n"
                    script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'depth <= $1.max_depth');\n"
                    script = script + "        END IF;\n"
                    script = script + "      END IF;\n\n"


                script = script + "      -- Process visibility restriction\n"
                script = script + "      IF p_min_visibility IS NOT NULL OR p_max_visibility IS NOT NULL THEN\n"
                script = script + "        v_parameters.min_visibility = p_min_visibility;\n"
                script = script + "        v_parameters.max_visibility = p_max_visibility;\n"
                script = script + "        IF v_parameters.min_visibility IS NOT NULL AND v_parameters.max_visibility IS NOT NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'visibility BETWEEN $1.min_visibility AND $1.max_visibility');\n"
                script = script + "        ELSIF v_parameters.min_visibility IS NOT NULL AND v_parameters.max_visibility IS NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'visibility >= $1.min_visibility');\n"
                script = script + "        ELSIF v_parameters.min_visibility IS NULL AND v_parameters.max_visibility IS NOT NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'visibility <= $1.max_visibility');\n"
                script = script + "        END IF;\n"
                script = script + "      END IF;\n\n"

    
                script = script + "      -- Process security restriction\n"
                script = script + "      IF p_min_security IS NOT NULL OR p_max_security IS NOT NULL THEN\n"
                script = script + "        v_parameters.min_security = p_min_security;\n"
                script = script + "        v_parameters.max_security = p_max_security;\n"
                script = script + "        IF v_parameters.min_security IS NOT NULL AND v_parameters.max_security IS NOT NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'security BETWEEN $1.min_security AND $1.max_security');\n"        
                script = script + "        ELSIF v_parameters.min_security IS NOT NULL AND v_parameters.max_security IS NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'security >= $1.min_security');\n"                
                script = script + "        ELSIF v_parameters.min_security IS NULL AND v_parameters.max_security IS NOT NULL THEN\n"
                script = script + "          v_where_conditions = ARRAY_APPEND(v_where_conditions, 'security <= $1.max_security');\n"                
                script = script + "        END IF;\n"
                script = script + "      END IF;\n\n"   

                script = script + "      -- Process table restriction\n"
                script = script + "      IF p_table_restriction IS NOT NULL THEN\n"
                script = script + "        v_refined = ''''||p_table_restriction||'''';\n"
                script = script + "        v_refined = REPLACE(v_refined,',',''',''');\n"
                script = script + "        v_refined = 'table_name IN('||v_refined||')';\n"
                script = script + "        v_where_conditions = ARRAY_APPEND(v_where_conditions, v_refined);\n"                
                script = script + "      END IF;\n\n"

                script = script + "     -- Look for phrases in text string\n"
                script = script + "     IF v_refined_search_text IS NOT NULL THEN\n"
                script = script + "       FOR v_phrase IN EXECUTE('select regexp_matches($1,''\"((?:[^\"]+|\"\")*)\"(?!\")'',''g'')') USING v_refined_search_text LOOP\n"                
                script = script + "          v_refined = translate(v_phrase,'{}','\"\"');\n"
                script = script + "          v_refined = replace(v_refined,'\"','');\n"
                script = script + "          v_refined = lower(ltrim(rtrim(v_refined)));\n"
                script = script + "          IF v_refined IS NOT NULL THEN\n"                
                script = script + "            v_refined = 'search ilike ''%'||v_refined||'%''';\n"                
                script = script + "            v_where_conditions = ARRAY_APPEND(v_where_conditions, v_refined);\n"
                script = script + "          END IF;\n"
                script = script + "       END LOOP;\n"
                script = script + "     END IF;\n\n"
                

                script = script + "      -- Build order by clause\n"
                script = script + "      -- Options:\n"
                script = script + "      --   ranked\n"
                script = script + "      --   distanceAsc\n"
                script = script + "      --   distanceDesc\n"
                script = script + "      --   (null) No order by\n"    
                
                rankedOrderBy = getRankedOrderBy()
                script = script + "      IF  p_order_by IS NOT NULL THEN\n"
                script = script + "        IF p_order_by = 'ranked' THEN\n"
                script = script + "          v_order_by_clause = '%s';\n" %(rankedOrderBy)
                
                if self.coordinates:
                    script = script + "        ELSIF p_order_by='distanceAsc' THEN\n"
                    script = script + "          v_order_by_clause = '%d ASC';\n" %(distanceIdx)
                    script = script + "        ELSIF p_order_by='distanceDesc' THEN\n"
                    script = script + "          v_order_by_clause = '%d DESC';\n" %(distanceIdx)
                script = script + "        END IF;\n"
                script = script + "      END IF;\n\n"
                script = script + "      v_sql = shared.make_sql_statement(v_select_clause,v_from_clause,v_where_conditions,v_order_by_clause);\n\n"
                script = script + "      raise notice '%',v_sql;\n"
                script = script + "      RETURN QUERY EXECUTE v_sql USING v_parameters;\n\n"                                
                script = script + "    END IF;\n"   
                script = script + "  END;\n"
                script = script + "$BODY$\n"
                script = script + "  LANGUAGE 'plpgsql' STABLE;\n\n"

                return (script)
            
            
            def getWrapperFunctions():
                script=""
                if self.coordinates:
                    # This is a wrapper to exclude the "geometry" column, that may cause issue for some apps
                    paramList = getParamList(False,None)
                    script = script + "CREATE OR REPLACE FUNCTION working.search_%s(%s)\n" %(self.name,paramList)
                    
                    sig =             "RETURNS SETOF search.%s_result AS\n" %(self.name)
                    sig = sig + "$BODY$\n"
                    script=script + sig
                    script=script + "  BEGIN\n"                                                            
                    top =       "    RETURN QUERY SELECT * FROM working.search_%s(p_search_text,\n" %(self.name)
                
                    if self.highLowFilter:
                        top = top + "      p_filter_name,\n"
                    
                    if self.urn:
                        top = top + "      p_search_urn,\n"
                    
                    if len(self.zones)>0:
                        top = top + "      p_min_zone,\n"
                        top = top + "      p_max_zone,\n"
                        
                    if self.ctree:
                        top = top + "      p_min_depth,\n"
                        top = top + "      p_max_depth,\n"
                        top = top + "      p_ancestor,\n"
    
                    if self.coordinates:
                        top = top + "      p_anchor_x,\n"
                        top = top + "      p_anchor_y,\n"
                    
                    script=script+top
                    
                    if self.coordinates:
                        script = script + "      NULL,\n"
                    
                    tail =        "      p_min_visibility,\n"
                    tail = tail + "      p_max_visibility,\n"
                    tail = tail + "      p_min_security,\n"
                    tail = tail + "      p_max_security,\n"
                    tail = tail + "      p_order_by,\n"
                    tail = tail + "      p_row_limit,\n"
                    tail = tail + "      p_table_restriction,\n"
                    tail = tail + "      p_application_name,\n"
                    tail = tail + "      p_username);\n"                    
                    tail = tail + "  END;\n"
                    tail = tail + "$BODY$\n"
                    tail = tail + "  LANGUAGE 'plpgsql';\n\n"
                    script=script+tail 
                    
                    
                    #This is a wrapper to allow simple point/radius restriction...
                    paramList = getParamList(False,"p_x numeric,p_y numeric,p_radius numeric")
                    script = script + "CREATE OR REPLACE FUNCTION working.search_%s(%s)\n" %(self.name,paramList)
                    script = script + sig
                    script = script + "  DECLARE\n"
                    script = script + "    v_circle geometry;\n"
                    script = script + "  BEGIN\n\n"
                    script = script + "    IF p_x IS NOT NULL AND p_y IS NOT NULL AND p_radius IS NOT NULL THEN\n"                    
                    script = script + "      v_circle = ST_Buffer(ST_GeomFromText('POINT('||p_x||' '||p_y||')',%d), p_radius);\n" %(srid)
                    script = script + "    END IF;\n\n"
                    script = script + top
                    script = script + "      v_circle,\n"
                    script = script + tail
               # "p_centre_x numeric, p_centre_y numeric, p_radius numeric"
                    return(script)

            # Produce all the various objects to facilitate a search  
            script = getDomainSearchResultType()                    
            script = script + getDomainSearchParametersType()
                        
            if self.highLowFilter:
                script = script + getTextFilter('high')
                script = script + getTextFilter('low')
                if self.coordinates:
                    script = script + getAreaFilter('high')
                    script = script + getAreaFilter('low')
                if self.ctree:
                    script = script + getAncestorFilter('high')
                    script = script + getAncestorFilter('low')
                    
                    
            else:
                script = script + getTextFilter(None)
                if self.coordinates:
                    script = script + getAreaFilter(None)
                if self.ctree:
                    script = script + getAncestorFilter(None)
            
            
            script = script + getDomainSearchCore()
            if self.coordinates:
                script = script + getWrapperFunctions()
            return(script)
                        
                            
        def getFunctionScript(suffix):

            params=[]
            params.append(("record_id","bigint",False))
            params.append(("table_name","character varying",False))
            
            if self.recordRanking:
                params.append(("fixed_ranking_offset","integer",True))
            
            params.append(("label","character varying",True))
            params.append(("search","character varying",True))

            if self.urn:
                params.append(("urn","bigint",True))

            if self.icon:
                params.append(("icon","character varying",True))

            if self.synopsis:
                params.append(("synopsis","character varying",True))
            
            if self.classification:
                params.append(("classification","character varying",True))
            
            if self.resultScript:
                params.append(("result_script","character varying",True))
            
            if self.ctree:
                params.append(("depth","integer",True))
                params.append(("immediate_ancestor",self.ctreeDataType,True))
                params.append(("root_ancestor",self.ctreeDataType,True))
                params.append(("descendant_count","integer",True))
            
            if len(self.zones)>0:
                params.append(("zone","integer",True))

            if self.coordinates:
                params.append(("x","numeric",True))
                params.append(("y","numeric",True))
                
                if self.positionalAccuracy:
                    params.append(("positional_accuracy","integer",True))
                
            params.append(("visibility","integer",True))
            params.append(("security","integer",True))
            
            if self.labelRanking:
                params.append(("label_ranking","integer",True))
            
            targetTable = self.name           
            if suffix is not None:
                targetTable=targetTable+"_%s" %(suffix)
            
            script = "CREATE OR REPLACE FUNCTION search.apply_%s_entry(\n" %(targetTable)

            i=0
            for thisParam in params:
                i=i+1
                paramLine = "  p_%s %s" %(thisParam[0], thisParam[1])
                if i<len(params):
                    paramLine =paramLine +",\n"                    
                script = script+ paramLine
                        
            script = script+") RETURNS void AS $$\n"
            script = script+"DECLARE\n"
            script = script+"  v_row_count integer;\n"
            script = script+"BEGIN\n\n"
            script = script+"  UPDATE search.%s SET" %(targetTable)
            
            i=0
            for thisParam in params:
                if thisParam[2]:
                    i=i+1
                    if i==1:
                        paramLine="\n    %s = p_%s" %(thisParam[0], thisParam[0])   
                    else:
                        paramLine = ",\n    %s = p_%s" %(thisParam[0], thisParam[0])                 
                    script = script+ paramLine
            
            script = script+ "\n  WHERE record_id = p_record_id\n"            
            script = script+ "  AND table_name = p_table_name;\n\n"
            script = script+ "  GET DIAGNOSTICS v_row_count = ROW_COUNT;\n\n"
            
            script = script+ "  IF v_row_count = 0 THEN\n"
            script = script+"    INSERT INTO search.%s (\n" %(targetTable)            
            script = script+"      record_id,\n"
            script = script+"      table_name"

            for thisParam in params:
                if thisParam[2]:
                    paramLine = ",\n      %s" %(thisParam[0])                 
                    script = script+ paramLine

            script = script+ ")\n    VALUES (\n"
            script = script+"      p_record_id,\n"
            script = script+"      p_table_name"

            for thisParam in params:
                if thisParam[2]:
                    paramLine = ",\n      p_%s" %(thisParam[0])                 
                    script = script+ paramLine

            script = script+");\n"
            script = script+"  END IF;\n"              
            script = script+"END;\n"
            script = script+"$$ LANGUAGE plpgsql;\n\n"
            
            
            return (script)

        
        script=         "-- Search domain\n"
        script=script + "-- -------------\n\n"
        script=script + "SELECT search.register_domain('%s',true,'%s');\n\n" %(self.name, configLocation)
        
 
        if self.highLowFilter:
            script=script+getTableScript("low")
            script=script+getFunctionScript("low")
            script=script+getTableScript("high")
            script=script+getFunctionScript("high")
            if self.orderByColumnCount is not None or self.orderByStartingType is not None:
                script=script+getOrderByFunctionScript()
            script=script+getTriggerFunctionScript()
            script=script+getTriggerScript("low")
            script=script+getTriggerScript("high")
        else:            
            script=script+getTableScript(None)
            script=script+getFunctionScript(None)
            if self.orderByColumnCount is not None or self.orderByStartingType is not None:
                script=script+getOrderByFunctionScript()
            script=script+getTriggerFunctionScript()
            script=script+getTriggerScript(None)
     
        script=script+getWithinZoneScript(settings)
        script=script+getDataExistsScript()

        script=script+getSearchFunctionsScript()
                
        
        return(script)



    def makeBuildScript(self, settings, configLocation):
        appLogger = settings.appLogger
        
        
        appLogger.info("")
        appLogger.info("Making search domain database build script")
        appLogger.info("------------------------------------------")
        
            
        outputFilename= os.path.join(settings.paths["repository"],"scripts", "generated",  "search domain files", self.name, "sql", "install", "build_%s.sql"%(self.name))             
        
        script=self.getPostgreSQLBuildScript(configLocation, settings, appLogger)
            
        file=open(outputFilename,"w")
        file.write(script)
        file.close()

        filename= os.path.join(settings.paths["repository"],"scripts", "generated",  "search domain files", self.name, "sql", "indexes", "drop_search_%s_indexes.sql" %(self.name))             
        dropFile=open(filename,"w")


        filename= os.path.join(settings.paths["repository"],"scripts", "generated",  "search domain files", self.name, "sql", "indexes", "create_search_%s_indexes.sql" %(self.name))             
        createFile=open(filename,"w")
                   
        for thisIndex in self.indexList:
            dropFile.write("DROP INDEX IF EXISTS %s.%s;\n" %(thisIndex[3], thisIndex[1]))                        
            createFile.write("%s\n" %(thisIndex[2]))

    
        dropFile.close();    
        createFile.close();
        
        return BuildScriptResult(filename=outputFilename, errorsFound=False, warningsFound=False)

        
    def buildRecordFormatterScripts(self, settings, dataSpecificationName, tableName, searchConfig, domainName, appLogger):
        
        scriptFilename= os.path.join(settings.paths["repository"], "scripts", "generated", "specification files", dataSpecificationName, "py", "search formatting", "%s_search_formatter.py"%(tableName))        
        scriptFile=open(scriptFilename,"w")
        
        standardArgs="dbCursor, raw, entry, optionSets"
        
        header=""        
        header = header + "# Generated search formatter methods\n"
        header = header + "# ----------------------------------\n\n"
        header = header + "# Methods to take raw data from a database record and produce formatted data for use in a search environment\n\n"
        
        if domainName is not None:                
            header = header + "# File tailored using '%s' search domain definition. \n\n" %(domainName)
                
        
        body =  "class DefaultSearchProcessor():\n\n"
        

        body = body + "\tdef getLabel(self, %s):\n" %(standardArgs)
        body = body + searchConfig.labelAssembler.getDefaultScript()
        

        body = body + "\tdef getSearch(self, %s):\n" %(standardArgs)
        body = body + searchConfig.searchAssembler.getDefaultScript()

        if self.urn:
            body = body + "\tdef getUrn(self, %s):\n" %(standardArgs)
            body = body + searchConfig.urnAssembler.getDefaultScript()
        
        if self.icon:
            body = body + "\tdef getIcon(self, %s, iconInfo):\n" %(standardArgs)
            body = body + searchConfig.iconAssembler.getDefaultScript()

        
        if self.highLowFilter:
            body = body + "\tdef getFilter(self, %s):\n" %(standardArgs)
            body = body + searchConfig.filterAssembler.getDefaultScript()
            

        if self.resultScript:
            body = body + "\tdef getResultScript(self, %s):\n" %(standardArgs)
            body = body + searchConfig.resultScriptAssembler.getDefaultScript()
        else:
            pass

        if self.ctree:
            body = body + "\tdef getCtree(self, %s):\n" %(standardArgs)
            body = body + searchConfig.ctreeAssembler.getDefaultScript(None)
        else:
            pass

        if self.coordinates:
            body = body + "\tdef getCoordinates(self, %s):\n" %(standardArgs)
            body = body + searchConfig.coordinatesAssembler.getDefaultScript()
        else:
            pass
        
        
        if self.positionalAccuracy:
            body = body + "\tdef getPositionalAccuracy(self, %s):\n" %(standardArgs)
            body = body + searchConfig.positionalAccuracyAssembler.getDefaultScript()
        else:
            pass


        if self.recordRanking:
            body = body + "\tdef getRecordRanking(self, %s):\n" %(standardArgs)
            body = body + "\t\trecordRanking=%d\n" %(searchConfig.ranking)
            body = body + "\t\treturn(recordRanking)\n\n"
        else:
            pass


        if self.labelRanking:
            body = body + "\tdef getLabelRanking(self, %s):\n" %(standardArgs)
            body = body + searchConfig.labelRankingAssembler.getDefaultScript()
            
        else:
            pass
 

        if self.synopsis:
            body = body + "\tdef getSynopsis(self, %s):\n" %(standardArgs)
            body = body + searchConfig.synopsisAssembler.getDefaultScript()

        else:
            pass

        if self.classification:
            body = body + "\tdef getClassification(self, %s):\n" %(standardArgs)
            body = body + searchConfig.classificationAssembler.getDefaultScript()

        else:
            pass



        # 
        body = body + "\tdef getVisibility(self, %s, defaultVisibility):\n" %(standardArgs)
        body = body + "\t\tif raw[\"visibility\"] is not None:\n"        
        body = body + "\t\t\tvisibility = raw[\"visibility\"]\n"
        body = body + "\t\telse:\n"
        body = body + "\t\t\tvisibility = defaultVisibility\n"
        body = body + "\t\treturn(visibility)\n\n"


        body = body + "\tdef getSecurity(self, %s, defaultSecurity):\n" %(standardArgs)
        body = body + "\t\tif raw[\"security\"] is not None:\n"        
        body = body + "\t\t\tsecurity = raw[\"security\"]\n"
        body = body + "\t\telse:\n"
        body = body + "\t\t\tsecurity = defaultSecurity\n"
        body = body + "\t\treturn(security)\n\n"


        body = body + "\tdef getRetain(self, %s):\n" %(standardArgs)
        body = body + "\t\treturn(True)\n\n"

        
        scriptFile.write(header+body)        

        scriptFile.close()
            

# ---------------------------------------------------------------

def processSynchronizeSearchSource(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):
    loopCursor = loopConnection.makeCursor("loopCursor", True, True)
    domainName = args["domainName"]        
    sourceType = args["sourceType"]
    sourceSchema = args["sourceSchema"]
    sourceName = args["sourceName"]
    sourceSpecification = args["specificationName"]
    lastSynchronized = args["lastSynchronized"] 
    configLocation = args["configLocation"]            
    repositoryPath = settings.env["repositoryPath"]
    appLogger = settings.appLogger
    recordLimit = args["recordLimit"]
    if recordLimit is not None:
        recordLimit=int(recordLimit)
    reportFrequency = int(settings.env["searchProgressReportFrequency"])
    
    def getApplyDml(prefix):
        dml='select search.apply_%s' %(domainName)
        if prefix is not None:
            dml=dml+"_%s" %(prefix)
        dml=dml+"_entry("

        paramCount=6

        if domainConfig.recordRanking:
            paramCount=paramCount+1

        if domainConfig.urn:
            paramCount=paramCount+1

        if domainConfig.icon:
            paramCount=paramCount+1
        
        if domainConfig.synopsis:
            paramCount=paramCount+1
            
        if domainConfig.classification:
            paramCount=paramCount+1        

        if domainConfig.resultScript:
            paramCount=paramCount+1        

        if domainConfig.ctree:
            paramCount=paramCount+4      


        if len(domainConfig.zones)>0:
            paramCount=paramCount+1        
       
        if domainConfig.coordinates:
            paramCount=paramCount+2        

            if domainConfig.positionalAccuracy:
                paramCount=paramCount+1        
                        
        if domainConfig.labelRanking:
            paramCount=paramCount+1        

        for i in range(0,paramCount):
            if i==0:
                dml=dml+"%s"
            else:
                dml=dml+",%s"        
        dml=dml+")"
        return(dml)
    

    def getFormatterFunctions(dataSpecification, tableName):

        getUrn = None
        getIcon = None
        getLabel = None
        getSearch = None
        getFilter = None
        getResultScript = None
        getCtree = None
        getCoordinates = None
        getPositionalAccuracy = None
        getRecordRanking = None
        getLabelRanking = None
        getSynopsis = None
        getClassification = None
        getVisibility = None
        getSecurity = None
        getRetain = None

        # Grab the default "generated" class
        
        moduleFilename = cs.getChimpScriptFilenameToUse(repositoryPath, ["specification files", dataSpecification, "py", "search formatting"], "%s_search_formatter.py" %(tableName))
        module = imp.load_source("%s_search_formatter.py" %(tableName), moduleFilename)
        defaultFunctions = module.DefaultSearchProcessor()

        if hasattr(defaultFunctions, 'getUrn'):
            getUrn = defaultFunctions.getUrn

        if hasattr(defaultFunctions, 'getIcon'):
            getIcon = defaultFunctions.getIcon

        if hasattr(defaultFunctions, 'getLabel'):
            getLabel = defaultFunctions.getLabel

        if hasattr(defaultFunctions, 'getSearch'):
            getSearch = defaultFunctions.getSearch
        
        if hasattr(defaultFunctions, 'getFilter'):
            getFilter = defaultFunctions.getFilter
        
        if hasattr(defaultFunctions, 'getResultScript'):
            getResultScript = defaultFunctions.getResultScript

        if hasattr(defaultFunctions, 'getCtree'):
            getCtree = defaultFunctions.getCtree
        
        if hasattr(defaultFunctions, 'getCoordinates'):
            getCoordinates = defaultFunctions.getCoordinates
        
        if hasattr(defaultFunctions, 'getPositionalAccuracy'):
            getPositionalAccuracy = defaultFunctions.getPositionalAccuracy

        if hasattr(defaultFunctions, 'getRecordRanking'):
            getRecordRanking = defaultFunctions.getRecordRanking

        if hasattr(defaultFunctions, 'getLabelRanking'):
            getLabelRanking = defaultFunctions.getLabelRanking

        if hasattr(defaultFunctions, 'getSynopsis'):
            getSynopsis = defaultFunctions.getSynopsis
        
        if hasattr(defaultFunctions, 'getClassification'):
            getClassification = defaultFunctions.getClassification

        if hasattr(defaultFunctions, 'getVisibility'):
            getVisibility = defaultFunctions.getVisibility

        if hasattr(defaultFunctions, 'getSecurity'):
            getSecurity = defaultFunctions.getSecurity

        if hasattr(defaultFunctions, 'getRetain'):
            getRetain = defaultFunctions.getRetain
            
        return ((getUrn,getIcon,getLabel,getSearch,getFilter,getResultScript,getCtree,getCoordinates,getPositionalAccuracy,getRecordRanking,getLabelRanking,getSynopsis,getClassification,getVisibility,getSecurity,getRetain))

    

    def getTableDefaults():
        sql="select %s.get_%s_default_visibility(),%s.get_%s_default_security()" %(sourceSchema, sourceName, sourceSchema, sourceName)
        supportCursor.execute(sql)
        defaults=supportCursor.fetchone()            
        return((defaults[0],defaults[1]))

    def getViewDefaults(entityName):
        sql="select mv.get_%s_default_visibility(),mv.get_%s_default_security()" %(entityName, entityName)
        supportCursor.execute(sql)
        defaults=supportCursor.fetchone()            
        return((defaults[0],defaults[1]))
        
    appLogger.info("")
    appLogger.info("Synchronizing search domain")
    appLogger.info("---------------------------")
    appLogger.info("  searchDomain        : %s" %(domainName))
        
  
    # Get domain settings
    if configLocation=="file":
        domainConfig = SearchDomain()
        domainConfig.setFromFile(settings, domainName)
    
    if len(domainConfig.zones) >0:
        hasZones=True
        domainConfig.calculateZoneExtents(supportCursor)
    else:
        hasZones=False

    domainConfig.debug(settings)
    
    appLogger.debug("")
    
    
    # Get processing functions for this source
    (getUrn,getIcon,getLabel,getSearch,getFilter,getResultScript,getCtree,getCoordinates,getPositionalAccuracy,getRecordRanking,getLabelRanking,getSynopsis,getClassification,getVisibility,getSecurity,getRetain) = getFormatterFunctions(sourceSpecification,sourceName)         
    if sourceType=="table":
        (defaultVisibility, defaultSecurity)  = getTableDefaults()
        modifiedColumnName = "mv_%s_modified" %(domainName)
    else:
        (defaultVisibility, defaultSecurity)  = getViewDefaults(sourceName)
        modifiedColumnName = "mv_%s_modified" %(domainName)

    # Off we go then...
    queue.startTask(taskId, True)
    
    

    # Estimate sizes        
    sql="select count(*) from %s.%s" %(sourceSchema,sourceName)
    if lastSynchronized is None:
        supportCursor.execute(sql)
    else:
        sql=sql+" where %s >" %(modifiedColumnName)
        sql=sql+"%s"
        supportCursor.execute(sql, (lastSynchronized,))    
    estimatedRows = supportCursor.fetchone()[0]
    queue.setScanResults(taskId, estimatedRows)
    
    
    # Identify all records to synchronize from this source
    
    optionSets = specification.getOptionSetsFromDatabase(supportCursor)
    if domainConfig.icon:
        iconInfo= specification.getIconInfo(sourceType,sourceName)
    else:
        iconInfo=None

    
    # Build main loop
    # ---------------
    sql = "select id"   
    if sourceType=="table":    
        for thisRecord in specification.records:        
            if thisRecord.table == sourceName:
                for thisAttribute in thisRecord.search.attributes:
                    sql = sql + ",%s" %(thisAttribute.column)            
                sql = sql +",visibility,security from %s.%s" %(sourceSchema,sourceName)
    elif sourceType=="view":
        for thisEntity in specification.entities:
            if thisEntity.name == sourceName:
                allColumns = thisEntity.getAllFinalColumns()
                for thisColumn in allColumns:
                    sql=sql+",%s" %(thisColumn)
                sql = sql +",visibility,security from %s.%s" %(sourceSchema,sourceName)
    if lastSynchronized is not None:
        sql = sql +" where %s>" %(modifiedColumnName)
        sql = sql + "%s"
                
    # Build delete statement (if entry is not to be retained)
    deleteStatements=[]
    if domainConfig.highLowFilter:
        # High
        deleteDml = "delete from search.%s_high where table_name=" %(domainName)
        deleteDml = deleteDml + "%s and record_id=%s"
        deleteStatements.append(deleteDml)
        
        # Low
        deleteDml = "delete from search.%s_low where table_name=" %(domainName)
        deleteDml = deleteDml + "%s and record_id=%s"
        deleteStatements.append(deleteDml)

    else:
        # None
        deleteDml = "delete from search.%s where table_name=" %(domainName)
        deleteDml = deleteDml + "%s and record_id=%s"
        deleteStatements.append(deleteDml)
        
    
    if lastSynchronized is None:
        loopCursor.execute(sql)
    else:
        loopCursor.execute(sql, (lastSynchronized,))
    


    # Pre-build DML statements
    dml=getApplyDml(None)
    appLogger.info("dml     : %s" %(dml))
    
    dmlHigh=getApplyDml("high")
    appLogger.info("dmlHigh : %s" %(dmlHigh))
    
    dmlLow=getApplyDml("low")
    appLogger.info("dmlLow  : %s" %(dmlLow))

                
    appLogger.debug("    Starting loop...")
    # For all records... pass through formatter class        
    
                                
                                # Main loop                                                
    lineCount = 0
    successCount = 0
    exceptionCount=0
    errorCount=0
    warningCount=0
    noticeCount=0
    ignoredCount=0   

    
    
    for data in loopCursor:
        
        if lineCount==0:
            appLogger.debug("    ...started")
        
        entry = SearchEntry(data["id"])    
        
        if domainConfig.coordinates:
            (x,y) = getCoordinates(dataCursor, data, entry, optionSets)
            entry.setCoordinates(x, y)

        if hasZones: 
            if  x is not None and y is not None:
                zone=None
                continueTesting=True
                for thisZone in domainConfig.zones:
                    if continueTesting:
                        if (x>=thisZone.xmin and x<=thisZone.xmax) and (y>=thisZone.ymin and y<=thisZone.ymax):
                            dataCursor.execute(thisZone.withinSql, (x,y))
                            within=dataCursor.fetchone()[0]
                            if within:
                                zone=thisZone.id
                            else:
                                continueTesting=False
                        else:
                            continueTesting=False
                if zone is None:
                    zone = domainConfig.zoneDefaultId
            else:
                zone = domainConfig.zoneDefaultId
            entry.setZone(zone)
                    
        if domainConfig.positionalAccuracy:
            entry.setPositionalAccuracy(getPositionalAccuracy(dataCursor, data, entry, optionSets))

        if domainConfig.recordRanking:
            entry.setRecordRanking(getRecordRanking(dataCursor, data, entry, optionSets))
            
        if domainConfig.labelRanking:
            entry.setLabelRanking(getLabelRanking(dataCursor, data, entry, optionSets))
                        
        if domainConfig.synopsis:
            entry.setSynopsis(getSynopsis(dataCursor, data, entry, optionSets))

        if domainConfig.classification:
            entry.setClassification(getClassification(dataCursor, data, entry, optionSets))

        if domainConfig.resultScript:
            entry.setResultScript(getResultScript(dataCursor, data, entry, optionSets))

        if domainConfig.ctree:
            (depth,immediateAncestor,rootAncestor,descendantCount)= getCtree(dataCursor, data, entry, optionSets)
            entry.setCtree(depth,immediateAncestor,rootAncestor,descendantCount)

        if domainConfig.urn:
            entry.setUrn(getUrn(dataCursor, data, entry, optionSets))

        if domainConfig.icon:
            entry.setIcon(getIcon(dataCursor, data, entry, optionSets, iconInfo))
                                                       
        entry.setVisibility(getVisibility(dataCursor, data, entry, optionSets, defaultVisibility))
        entry.setSecurity(getSecurity(dataCursor, data, entry, optionSets, defaultSecurity))    

        if domainConfig.highLowFilter:
            entry.setFilter(getFilter(dataCursor, data, entry, optionSets))
            if entry.filter == "high":
                sql = dmlHigh
            else:
                sql = dmlLow
        else:
            sql = dml
            
        entry.setLabel(getLabel(dataCursor, data, entry, optionSets)) 
        
        searchValue = getSearch(dataCursor, data, entry, optionSets)
        if searchValue is not None: 
                        
            entry.setSearch(searchValue)
            retain = getRetain(dataCursor, data, entry, optionSets)

            if retain:
                row=entry.getTableRow(domainConfig, sourceName)
                try:
                    dataCursor.execute(sql, row)
                    successCount = successCount + 1
                except Exception as details:
                    print(sql)
                    print(row)
                    raise
                
            else:    
                for thisDeleteStatement in deleteStatements:
                    dataCursor.execute(thisDeleteStatement, (sourceName, data["id"] ))
                ignoredCount = ignoredCount +1
        else:
            warningCount = warningCount + 1
        
        if lineCount % reportFrequency == 0:                
            queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
        lineCount = lineCount + 1                            

        
        if  recordLimit is not None:
            entry.debug(appLogger)
            if lineCount==recordLimit:
                break
    
    #chimpqueue.finishTask(supportConnection, supportCursor, taskId, True, True, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                    
    loopCursor.close()
    supportConnection.connection.commit()
        
    appLogger.info("")
    
    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )










#def processDomain(settings, resourceRoot, searchDomain, searchIndexStrategy, db, recordLimit, appLogger):
#    
#    def resetDomainSources():
#        sql = "select search.reset_domain_sources(%s)"                
#        db.data.execute(sql, (searchDomain,))
#        db.dataCommit()
#
#    def registerSyncStart(schemaName, databaseObjectName):
#        sql = "select search.register_sync_start(%s,%s,%s)"                
#        db.data.execute(sql, (searchDomain,schemaName,databaseObjectName))
#        db.dataCommit()
#
#    def registerSyncEstimation(schemaName, databaseObjectName, estimatedRowCount):
#        sql = "select search.register_sync_estimation(%s,%s,%s,%s)"                
#        db.data.execute(sql, (searchDomain,schemaName,databaseObjectName,estimatedRowCount))
#        timestamp = db.data.fetchone()[0]
#        db.dataCommit()
#        return(timestamp)
#
#    def registerSyncProgress(schemaName, databaseObjectName, progressCount):
#        sql = "select search.register_sync_progress(%s,%s,%s,%s)"                
#        db.data.execute(sql, (searchDomain,schemaName,databaseObjectName,progressCount))
#        db.dataCommit()
#
#    def registerSyncFinish(schemaName, databaseObjectName, progressCount, startTime):
#        sql = "select search.register_sync_finish(%s,%s,%s,%s,%s)"                
#        db.data.execute(sql, (searchDomain,schemaName,databaseObjectName,progressCount,startTime))
#        db.dataCommit()
#    
#    def getApplyDml(prefix):
#        dml='select search.apply_%s' %(searchDomain)
#        if prefix is not None:
#            dml=dml+"_%s" %(prefix)
#        dml=dml+"_entry("
#
#        paramCount=7
#
#        if domainConfig.urn:
#            paramCount=paramCount+1
#
#        if domainConfig.icon:
#            paramCount=paramCount+1
#        
#        if domainConfig.synopsis:
#            paramCount=paramCount+1
#            
#        if domainConfig.classification:
#            paramCount=paramCount+1        
#
#        if domainConfig.resultScript:
#            paramCount=paramCount+1        
#
#        if domainConfig.ctree:
#            paramCount=paramCount+4      
#
#
#        if len(domainConfig.zones)>0:
#            paramCount=paramCount+1        
#       
#        if domainConfig.coordinates:
#            paramCount=paramCount+2        
#
#            if domainConfig.positionalAccuracy:
#                paramCount=paramCount+1        
#                        
#        if domainConfig.labelRanking:
#            paramCount=paramCount+1        
#
#        for i in range(0,paramCount):
#            if i==0:
#                dml=dml+"%s"
#            else:
#                dml=dml+",%s"        
#        dml=dml+")"
#        return(dml)
#    
#    def getMainLoopSql(domainSourceRecord, specification, modifiedColumnName):
#        selectList = "select c_id"
#
#        # Build loop from table columns
#        # -----------------------------        
#        if domainSourceRecord["database_object_type"] == "table":
#            for thisRecord in specification.records:
#                
#                if thisRecord.table == domainSourceRecord["database_object_name"]:
#                    for thisAttribute in thisRecord.search.attributes:
#                        selectList = selectList + ",%s AS %s" %(thisAttribute.actualColumnName,cs.removePrefix(thisAttribute.actualColumnName))
#                    
#                    selectList=selectList+",w_visibility AS visibility,w_security AS security from %s.%s where %s>" %(domainSourceRecord["schema_name"],thisRecord.table,modifiedColumnName)
#                    selectList=selectList+"%s"
#
#        # Build loop from view columns
#        # ----------------------------
#        if domainSourceRecord["database_object_type"] == "view":
#            for thisEntity in specification.entities:                
#                if thisEntity.name == domainSourceRecord["database_object_name"]:
#                    
#                    
#                    
#                    allColumns = thisEntity.getAllFinalColumns()
#                    for thisColumn in allColumns:
#                        selectList = selectList + ",%s AS %s" %(thisColumn, cs.removePrefix(thisColumn))
#                    
#                    selectList=selectList+",w_visibility AS visibility,w_security AS security from %s.%s where %s>" %(domainSourceRecord["schema_name"],thisEntity.name,modifiedColumnName)
#                    selectList=selectList+"%s"
#
#        return(selectList)
#
#    def getFormatterFunctions(dataSpecification, tableName):
#
#        getUrn = None
#        getIcon = None
#        getLabel = None
#        getSearch = None
#        getFilter = None
#        getResultScript = None
#        getCtree = None
#        getCoordinates = None
#        getPositionalAccuracy = None
#        getRecordRanking = None
#        getLabelRanking = None
#        getSynopsis = None
#        getClassification = None
#        getVisibility = None
#        getSecurity = None
#
#        # Grab the default "generated" class
#        moduleFilename = os.path.join(resourceRoot, "specifications", dataSpecification, "search config", "default_%s_search_formatter.py" %(tableName))
#        module = imp.load_source("default_%s_search_formatter" %(tableName), moduleFilename)
#        defaultFunctions = module.DefaultSearchProcessor()
#
#        if hasattr(defaultFunctions, 'getUrn'):
#            getUrn = defaultFunctions.getUrn
#
#        if hasattr(defaultFunctions, 'getIcon'):
#            getIcon = defaultFunctions.getIcon
#
#        if hasattr(defaultFunctions, 'getLabel'):
#            getLabel = defaultFunctions.getLabel
#
#        if hasattr(defaultFunctions, 'getSearch'):
#            getSearch = defaultFunctions.getSearch
#        
#        if hasattr(defaultFunctions, 'getFilter'):
#            getFilter = defaultFunctions.getFilter
#        
#        if hasattr(defaultFunctions, 'getResultScript'):
#            getResultScript = defaultFunctions.getResultScript
#
#        if hasattr(defaultFunctions, 'getCtree'):
#            getCtree = defaultFunctions.getCtree
#        
#        if hasattr(defaultFunctions, 'getCoordinates'):
#            getCoordinates = defaultFunctions.getCoordinates
#        
#        if hasattr(defaultFunctions, 'getPositionalAccuracy'):
#            getPositionalAccuracy = defaultFunctions.getPositionalAccuracy
#
#        if hasattr(defaultFunctions, 'getRecordRanking'):
#            getRecordRanking = defaultFunctions.getRecordRanking
#
#        if hasattr(defaultFunctions, 'getLabelRanking'):
#            getLabelRanking = defaultFunctions.getLabelRanking
#
#        if hasattr(defaultFunctions, 'getSynopsis'):
#            getSynopsis = defaultFunctions.getSynopsis
#        
#        if hasattr(defaultFunctions, 'getClassification'):
#            getClassification = defaultFunctions.getClassification
#
#
#        if hasattr(defaultFunctions, 'getVisibility'):
#            getVisibility = defaultFunctions.getVisibility
#
#        if hasattr(defaultFunctions, 'getSecurity'):
#            getSecurity = defaultFunctions.getSecurity
#           
#        # Grab the custom and manually overload!!
#        moduleFilename = os.path.join(resourceRoot, "specifications", dataSpecification, "search config", "custom_%s_search_formatter.py" %(tableName))
#        module = imp.load_source("custom_%s_search_formatter" %(tableName), moduleFilename)
#        customFunctions = module.CustomSearchProcessor()
#
#        if hasattr(customFunctions, 'getUrn'):
#            getUrn = customFunctions.getUrn
#
#        if hasattr(customFunctions, 'getIcon'):
#            getIcon = customFunctions.getIcon
#        
#        if hasattr(customFunctions, 'getLabel'):
#            getLabel = customFunctions.getLabel
#
#        if hasattr(customFunctions, 'getSearch'):
#            getSearch = customFunctions.getSearch
#        
#        if hasattr(customFunctions, 'getFilter'):
#            getFilter = customFunctions.getFilter
#        
#        if hasattr(customFunctions, 'getResultScript'):
#            getResultScript = customFunctions.getResultScript
#
#        if hasattr(customFunctions, 'getCtree'):
#            getResultScript = customFunctions.getCtree
#        
#        if hasattr(customFunctions, 'getCoordinates'):
#            getCoordinates = customFunctions.getCoordinates
#        
#        if hasattr(customFunctions, 'getPositionalAccuracy'):
#            getPositionalAccuracy = customFunctions.getPositionalAccuracy
#
#        if hasattr(customFunctions, 'getRecordRanking'):
#            getRecordRanking = customFunctions.getRecordRanking
#
#        if hasattr(customFunctions, 'getLabelRanking'):
#            getLabelRanking = customFunctions.getLabelRanking
#
#        if hasattr(customFunctions, 'getSynopsis'):
#            getSynopsis = customFunctions.getSynopsis
#        
#        if hasattr(customFunctions, 'getClassification'):
#            getClassification = customFunctions.getClassification
#
#        if hasattr(customFunctions, 'getVisibility'):
#            getVisibility = customFunctions.getVisibility
#
#        if hasattr(customFunctions, 'getSecurity'):
#            getSecurity = customFunctions.getSecurity
#            
#        return ((getUrn,getIcon,getLabel,getSearch,getFilter,getResultScript,getCtree,getCoordinates,getPositionalAccuracy,getRecordRanking,getLabelRanking,getSynopsis,getClassification,getVisibility,getSecurity))
#
#    
#    def getDomainInfo():
#        sql="select synchronization_enabled, config_location from search.domains where domain=%s"
#        db.data.execute(sql, (searchDomain,))
#        domainRecord=db.data.fetchone()
#        
#        sql="select search.is_there_any_%s_data()" %(searchDomain)
#        db.data.execute(sql)
#        dataResult=db.data.fetchone()
#                            
#        return((domainRecord[0],domainRecord[1],dataResult[0]))
#
#
#    def getTableDefaults(recordName):
#        sql="select store.get_%s_default_visibility(),store.get_%s_default_security()" %(recordName, recordName)
#        db.data.execute(sql)
#        defaults=db.data.fetchone()            
#        return((defaults[0],defaults[1]))
#
#    def getViewDefaults(entityName):
#        sql="select mv.get_%s_default_visibility(),mv.get_%s_default_security()" %(entityName, entityName)
#        db.data.execute(sql)
#        defaults=db.data.fetchone()            
#        return((defaults[0],defaults[1]))
#
#    def dropIndexes():    
#        appLogger.debug("  Dropping indexes")
#        filename="drop_search_%s_indexes.sql" %(searchDomain)
#        filename= os.path.join(resourceRoot,"search domains", searchDomain, "database scripts", "index scripts", filename)
#        appLogger.debug("    Script: %s" %(filename))
#        dropFile=open(filename,"r")
#        for line in dropFile:
#            stripped = line.strip("\n")
#            appLogger.debug("      %s..." %(stripped))
#            db.data.execute(stripped)
#        dropFile.close()
#        
#
#    def createIndexes():    
#        appLogger.debug("  Create indexes")
#        filename="create_search_%s_indexes.sql" %(searchDomain)
#        filename= os.path.join(resourceRoot,"search domains", searchDomain, "database scripts", "index scripts", filename)
#        appLogger.debug("    Script: %s" %(filename))
#        createFile=open(filename,"r")
#        for line in createFile:
#            stripped = line.strip("\n")
#            appLogger.debug("      %s..." %(stripped))
#            db.data.execute(stripped)
#        createFile.close()        
#        
#    appLogger.info("")
#    appLogger.info("Synchronizing search domain")
#    appLogger.info("---------------------------")
#    appLogger.info("  searchDomain        : %s" %(searchDomain))
#    
#    if recordLimit is not None:
#        recordLimit=int(recordLimit)
#    
#    (domainEnabled, configLocation, dataExists) = getDomainInfo()
#    
#    appLogger.info("  domainEnabled       : %s" %(str(domainEnabled)))
#    appLogger.info("  configLocation      : %s" %(configLocation))
#    appLogger.info("  searchIndexStrategy : %s" %(str(searchIndexStrategy)))
#    appLogger.info("  dataExists          : %s" %(str(dataExists)))
#  
#    rebuildIndexes=False
#    if (searchIndexStrategy=="auto" and not dataExists) or searchIndexStrategy=="recreate":
#        dropIndexes()
#        rebuildIndexes=True 
#  
#  
#    # Find all sources contributing to this domain  
#    sql = "select schema_name, database_object_type, database_object_name, data_specification, last_synchronized from search.domain_sources where domain=%s and synchronization_enabled order by data_specification"
#    db.createDictionaryCursor()    
#    db.dictionaryCursor.execute(sql, (searchDomain,))
#    domainSources=db.dictionaryCursor.fetchall()
#    db.closeDictionaryCursor()
#
#    # Get domain settings
#    if configLocation=="file":
#        domainConfig = SearchDomain()
#        domainConfig.setFromFile(resourceRoot, searchDomain)
#    
#    if len(domainConfig.zones) >0:
#        hasZones=True
#        domainConfig.calculateZoneExtents(db)
#    else:
#        hasZones=False
#
#    domainConfig.debug(appLogger)
#    
#    appLogger.debug("")
#    
#    
#    # Reset stats for this domain
#    resetDomainSources()
#    
#    currentSpecification = None
#    
#    for thisSource in domainSources:
#
#
#        databaseObjectType = thisSource["database_object_type"]
#        schema = thisSource["schema_name"]
#        tableName = thisSource["database_object_name"]
#
#        appLogger.debug("  Source: %s" %(tableName))                
#        
#        # Get processing functions for this source
#        (getUrn,getIcon,getLabel,getSearch,getFilter,getResultScript,getCtree,getCoordinates,getPositionalAccuracy,getRecordRanking,getLabelRanking,getSynopsis,getClassification,getVisibility,getSecurity) = getFormatterFunctions(thisSource["data_specification"],tableName)         
#
#        if databaseObjectType=="table":
#            (defaultVisibility, defaultSecurity)  = getTableDefaults(tableName)
#            modifiedColumnName = "w_modified"
#        else:
#            (defaultVisibility, defaultSecurity)  = getViewDefaults(tableName)
#            modifiedColumnName = "mv_%s_modified" %(tableName)
#
#
#        registerSyncStart(schema, tableName)
#        
#
#
#        # Estimate sizes
#        
#        
#        
#        sql="select count(*) from %s.%s where %s >=" %(schema,tableName, modifiedColumnName)
#        sql=sql+"%s"
#        db.data.execute(sql, (thisSource["last_synchronized"],))
#        estimatedRows = db.data.fetchone()[0]
#        
#        startTimestamp = registerSyncEstimation(schema,tableName,estimatedRows)
#
#        
#        # Identify all records to synchronize from this source
#        
#        if currentSpecification is None or (currentSpecification is not None and thisSource["data_specification"] != currentSpecification):
#            specification = chimpspec.Spec(settings, resourceRoot, thisSource["data_specification"], None)
#            optionSets = specification.getOptionSetsFromDatabase(db.data)
#            if domainConfig.icon:
#                iconInfo= specification.getIconInfo(databaseObjectType,tableName)
#            else:
#                iconInfo=None
#        
#        sql = getMainLoopSql(thisSource, specification, modifiedColumnName)
#        db.createNamedDictionaryCursor()   
#        appLogger.debug("    Loop sql     : %s" %(sql))
#        db.namedDictionaryCursor.execute(sql, (thisSource["last_synchronized"],))
#
#        
#
#        # Pre-build DML statements
#        dml=getApplyDml(None)
#        appLogger.info("dml     : %s" %(dml))
#        
#        dmlHigh=getApplyDml("high")
#        appLogger.info("dmlHigh : %s" %(dmlHigh))
#        
#        dmlLow=getApplyDml("low")
#        appLogger.info("dmlLow  : %s" %(dmlLow))
#
#                    
#        appLogger.debug("    Starting loop...")
#        # For all records... pass through formatter class        
#        i = 0
#        for data in db.namedDictionaryCursor:
#            
#            if i==0:
#                appLogger.debug("    ...started")
#            entry = SearchEntry(data["c_id"])    
#            
#            if domainConfig.coordinates:
#                (x,y) = getCoordinates(db.data, data, entry, optionSets)
#                entry.setCoordinates(x, y)
#
#            if hasZones:
#                zone=None
#                continueTesting=True
#                for thisZone in domainConfig.zones:
#                    if continueTesting:
#                        if (x>=thisZone.xmin and x<=thisZone.xmax) and (y>=thisZone.ymin and y<=thisZone.ymax):
#                            db.data.execute(thisZone.withinSql, (x,y))
#                            within=db.data.fetchone()
#                            if within:
#                                zone=thisZone.id
#                            else:
#                                continueTesting=False
#                        else:
#                            continueTesting=False
#                if zone is None:
#                    zone = domainConfig.zoneDefaultId
#                entry.setZone(zone)
#                        
#            if domainConfig.positionalAccuracy:
#                entry.setPositionalAccuracy(getPositionalAccuracy(db.data, data, entry, optionSets))
#
#            if domainConfig.recordRanking:
#                entry.setRecordRanking(getRecordRanking(db.data, data, entry, optionSets))
#                
#            if domainConfig.labelRanking:
#                entry.setLabelRanking(getLabelRanking(db.data, data, entry, optionSets))
#                            
#            if domainConfig.synopsis:
#                entry.setSynopsis(getSynopsis(db.data, data, entry, optionSets))
#
#            if domainConfig.classification:
#                entry.setClassification(getClassification(db.data, data, entry, optionSets))
#
#            if domainConfig.resultScript:
#                entry.setResultScript(getResultScript(db.data, data, entry, optionSets))
#
#            if domainConfig.ctree:
#                (depth,immediateAncestorUrn,rootAncestorUrn,descendantCount)= getCtree(db.data, data, entry, optionSets)
#                entry.setCtree(depth,immediateAncestorUrn,rootAncestorUrn,descendantCount)
#
#            if domainConfig.urn:
#                entry.setUrn(getUrn(db.data, data, entry, optionSets))
#
#            if domainConfig.icon:
#                entry.setIcon(getIcon(db.data, data, entry, optionSets, iconInfo))
#                                                           
#            entry.setVisibility(getVisibility(db.data, data, entry, optionSets, defaultVisibility))
#            entry.setSecurity(getSecurity(db.data, data, entry, optionSets, defaultSecurity))
#
#            if domainConfig.highLowFilter:
#                entry.setFilter(getFilter(db.data, data, entry, optionSets))
#                if entry.filter == "high":
#                    sql = dmlHigh
#                else:
#                    sql = dmlLow
#            else:
#                sql = dml
#                
#            entry.setLabel(getLabel(db.data, data, entry, optionSets)) 
#            
#            searchValue = getSearch(db.data, data, entry, optionSets)
#            if searchValue is not None: 
#                entry.setSearch(searchValue)
#                    
#                row=entry.getTableRow(domainConfig, tableName)
#                db.data.execute(sql, row)
#                        
#        
#            
#            if i%settings.searchProgressReportFrequency==0:
#                registerSyncProgress(schema,tableName,i)
#    
#            i=i+1            
#            
#            if  recordLimit is not None:
#                entry.debug(appLogger)
#                if i==recordLimit:
#                    break
#        
#        db.closeNamedDictionaryCursor()
#                
#        registerSyncFinish(schema,tableName,i,startTimestamp)
#        print (tableName+": %d" %(i))
#        
#        db.dataCommit()
#        db.supportCommit()
#        
#    if rebuildIndexes:
#        createIndexes()
#        db.dataCommit()
#
#    appLogger.info("")
#    
#    return(False, False)
