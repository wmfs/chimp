REFERENCE_SCHEMA = "reference"

import cs
import chimpspec
import chimpsql

def formatDataForInsertStatement(columnDataType, stringValue):
    if stringValue is not None:
        if columnDataType in("integer", "bigint", "decimal", "double precision", "boolean"):
            value = stringValue
        elif columnDataType =="character varying":
            value = "'{0}'".format(stringValue)
    else:
        value = "NULL"
    return(value)
        

def turnNative(columnDataType, stringValue):
    if stringValue is not None:
        if columnDataType == "integer":
            value = int(stringValue)
        elif columnDataType == "bigint":
            value = int(stringValue)
        elif columnDataType in("decimal", "double precision"):
            value = float(stringValue)
        elif columnDataType=="boolean":
            if stringValue in("true","True","TRUE"):
                value = True
            elif stringValue in("false","False","FALSE"):
                value = False
        else:
            value = stringValue
    else:
        value = None
    return(value)
    
class Option:
        
    def __init__(self, columnDataType, optionTag, additionalAttributes):
        stringValue = str(cs.grabAttribute(optionTag,"value"))
        self.value = turnNative(columnDataType,stringValue)
        self.label=str(cs.grabAttribute(optionTag,"label"))
        
        self.attribData = []
        additionalTag = optionTag.getElementsByTagName("additional")
        if len(additionalTag)>0:
            additionalTag = additionalTag[0]
            attribTags = additionalTag.getElementsByTagName("attrib")
            i =0 
            for attrib in attribTags:
                columnDataType = additionalAttributes[i].columnDataType
                value = str(cs.grabAttribute(attrib,"value"))
                if value !="":
                    value = turnNative(columnDataType, value)
                else:
                    value = None
                self.attribData.append(value)
                i += 1        

class OptionSet:
    
    def debug(self, appLogger):
        appLogger.debug("  {0} -{1}:".format(self.name, self.field.strippedColumnClause("", False)))
        
        for attribute in self.additionalAttributes:
            appLogger.debug("    + {0} -{1}".format(attribute.column, attribute.strippedColumnClause("", False)))
        
        count = len(self.options)
                
        if count == 0:
            appLogger.debug("    <NO OPTIONS>")
        else:
            appLogger.debug("    Count = {0}".format(count))
            keys = sorted(self.options.keys())                    
            key1 = keys[0]
            key2 = keys[count-1]
            appLogger.debug("    [{0}] = [{1}]".format(key1, self.options[key1].label))
            
            if key1 != key2:
                appLogger.debug("    [{0}] = [{1}]".format(key2, self.options[key2].label))
        
        appLogger.debug("")


    def getOptionSetTable(self, specificationName):        
        tableName = "{1}".format(specificationName, self.name)
        
        additionalAttributes = "".join(map(lambda field: ",\n{0}".format(field.columnClause(None)), self.additionalAttributes)) 
        
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  id serial primary key NOT NULL,\n"
                "{2},\n"
                "  label character varying(500){3});\n\n".format(REFERENCE_SCHEMA, tableName, self.field.columnClause(None), additionalAttributes) )        
        return chimpsql.Table(tableName, REFERENCE_SCHEMA, ddl)

    def getOptionSetIndex(self, specificationName):
        tableName = "{1}".format(specificationName, self.name)
        indexName = "{1}_value_idx".format(specificationName, self.name)
        return chimpsql.Index(indexName, tableName, REFERENCE_SCHEMA,
                     "CREATE UNIQUE INDEX {0} ON {1}.{2} (value);\n\n".format(indexName, REFERENCE_SCHEMA, tableName))   


    def getInstallFunction(self, specificationName):
        functionName = "install_{1}".format(specificationName, self.name)        
        data = ""
        i=0
        keys=sorted(self.options)
        for key in keys:
            option = self.options[key]
            if i>0:
                data += ","
            i += 1
                        
            value=option.value
            if self.type == "text":
                value = "'{0}'".format(value.replace("'","''"))
            
            label = option.label.replace("'","''")
            
            attribs = ""
            j =0 
            for attrib in option.attribData:                
                attribs += ", "
                columnDataType = self.additionalAttributes[j].columnDataType
                
                if attrib is not None:
                    attribValue = str(attrib)
                                    
                    if columnDataType == "character varying":
                        attribValue = attribValue.replace("'","''")
                else:
                    attribValue=None                                            
                attribs += formatDataForInsertStatement(columnDataType,attribValue)
                j += 1


            
            data += "\n  ({0}, '{1}'{2})".format(value,label,attribs)

        additionalColumns = "".join(map(lambda field: ",\n   {0}".format(field.column), self.additionalAttributes)) 

        return chimpsql.Function(functionName, REFERENCE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS void AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  INSERT INTO {0}.{3}\n"
                         "  (value,\n"
                         "   label{4})\n"
                         "  VALUES{5};\n"                                                 
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(REFERENCE_SCHEMA, functionName, specificationName, self.name, additionalColumns, data))

    def getInsertDML(self, installFunction):
        return chimpsql.DML("SELECT {0}.{1}();\n\n".format(REFERENCE_SCHEMA, installFunction.name))

        
    def __init__(self, optionSetTag):

        self.name=cs.grabAttribute(optionSetTag,"name")        
        self.type=cs.grabAttribute(optionSetTag,"type")
        self.size=cs.grabAttribute(optionSetTag,"size")
        if self.size is not None:
            self.size=int(self.size)
            
        self.decimalPlaces=cs.grabAttribute(optionSetTag,"decimalPlaces")
        if self.decimalPlaces is not None:
            self.decimalPlaces = int(self.decimalPlaces)
        
        self.field = chimpspec.SpecificationRecordField(None, None, column="value", type=self.type, mandatory=True, size=self.size, decimalPlaces=self.decimalPlaces)

        self.additionalAttributes = []
        additionalAttributesTag = optionSetTag.getElementsByTagName("additionalAttributes")
        if len(additionalAttributesTag)>0:
            additionalAttributesTag = additionalAttributesTag[0]
            attributeTags = additionalAttributesTag.getElementsByTagName("attribute")
            for attribute in attributeTags:
                attributeName=cs.grabAttribute(attribute,"name")        
                attributeType=cs.grabAttribute(attribute,"type")
                attributeSize=cs.grabAttribute(attribute,"size")
                attributeDecimalPlaces=cs.grabAttribute(attribute,"decimalPlaces")
                if attributeSize is not None:
                    attributeSize=int(attributeSize)
                    
                attributeDecimalPlaces=cs.grabAttribute(attribute,"decimalPlaces")
                if attributeDecimalPlaces is not None:
                    attributeDecimalPlaces = int(attributeDecimalPlaces)
                
                self.additionalAttributes.append(chimpspec.SpecificationRecordField(None, None, column=attributeName, type=attributeType, mandatory=False, size=attributeSize, decimalPlaces=attributeDecimalPlaces))

        allOptions = optionSetTag.getElementsByTagName("option")            
        self.options={}
        for optionTag in allOptions:
            newOption=Option(self.field.columnDataType, optionTag, self.additionalAttributes)
            self.options[newOption.value] = newOption
                    