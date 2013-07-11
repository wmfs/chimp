import xml.dom.minidom
import cs
import re
import os.path
#import search
#import pinhead
import itertools
import calc
import imp
import optionsets
import alert

class AdditionalIndex():
    
    def __init__(self, indexTag):
        self.using = cs.grabAttribute(indexTag,"using")
        columnTags = indexTag.getElementsByTagName("column")
                
        self.columnList=[]
        for thisColumn in columnTags:                
            columnName =cs.grabAttribute(thisColumn,"name")                        
            self.columnList.append(columnName)
            
        self.commaDelimitedColumns = cs.delimitedStringList(self.columnList, ", ")
        self.underscoreDelimitedColumns = cs.delimitedStringList(self.columnList, "_")
    
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("       {0} (using={1})".format(self.columnList, self.using))

class MvModifiedActionJoin():
    def __init__(self, joinTag, record):
        self.localColumn = str(cs.grabAttribute(joinTag, "localColumn"))
        self.targetColumn = str(cs.grabAttribute(joinTag, "targetColumn"))
        self.localField = record.getField(self.localColumn)
        

class MvModifyingAction():

    def __init__(self, actionTag, record, appLogger):
        self.targetTable=str(cs.grabAttribute(actionTag, "targetTable"))
        self.entityName=str(cs.grabAttribute(actionTag, "entityName"))
        self.record = record
        self.joins=[]
                
        joinsTag=actionTag.getElementsByTagName("joins")
        if len(joinsTag)>0:
            joinsTag=joinsTag[0]
            joinTags=joinsTag.getElementsByTagName("join")
            for thisTag in joinTags:                
                self.joins.append(MvModifiedActionJoin(thisTag, record))
        if appLogger is not None:
            appLogger.debug("  MvModifyingAction: targetTable=%s entityName=%s joins=(%s)" %(self.targetTable, self.entityName, str(self.joins)))



class ColumnTransform:
     
    class ColumnFunction:
        def __init__(self, name, source, parameterValues, appLogger):
            self.name = name
            self.source = source
            self.parameterValues = parameterValues
            if appLogger is not None:
                appLogger.debug("      name            = %s" %(self.name))
                appLogger.debug("      source          = %s" %(self.source))
                appLogger.debug("      parameterValues = %s" %(self.parameterValues))
                appLogger.debug("")

    
    def __init__(self, chimpFunctions, columnTransformTag, appLogger):
        if appLogger is not None:
            appLogger.debug("  ColumnTransform record")
        
        self.column = cs.grabAttribute(columnTransformTag, "column")
        if appLogger is not None:
            appLogger.debug("    column    = %s" %(self.column))

        self.functions=[]
        allFunctions = columnTransformTag.getElementsByTagName("function")        
        for thisFunction in allFunctions:
            
            name =cs.grabAttribute(thisFunction,"name")

            source="native"
            for thisChimpFunction in chimpFunctions:                 
                if name== thisChimpFunction.name:
                    source="chimp"
            
            parameterValues=[]
            allParameterValues = thisFunction.getElementsByTagName("parameterValues")
            if len(allParameterValues)>0:
                allParameterValues = allParameterValues[0]
                allValues = thisFunction.getElementsByTagName("parameter")
                for thisValue in allValues:
                    parameterValues.append(str(cs.grabAttribute(thisValue,"value")))
                                    
            self.functions.append(self.ColumnFunction(name,source,parameterValues, appLogger))
        if appLogger is not None:
            appLogger.debug("")

class ModeIdentification:
    def __init__(self, modeIdentificationTag, appLogger):
        self.condition=cs.grabAttribute(modeIdentificationTag,"condition")                
        tablesTag=modeIdentificationTag.getElementsByTagName("tables")[0]
        tables=tablesTag.getElementsByTagName("table") 
        if appLogger is not None:               
            appLogger.debug("  Tables:")   
        self.tables=[]
        for thisTable in tables:            
            self.tables.append(cs.grabAttribute(thisTable,"name"))
        
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug("   Condition: "+cs.prettyNone(self.condition))            
            appLogger.debug("   Tables")            
        for thisTable in self.tables:
            if appLogger is not None:
                appLogger.debug("     %s" %(thisTable))            

        
        
class EntityRecord:
    
#
#    def fromAndWhereClauses(self, schemaName, indent, initialWhereCondition):
#
#        script = indent+"FROM"
#        i=0          
#        for thisEntityTable in self.tables:
#            i=i+1
#            tableLine="\n  %s%s.%s AS %s" %(indent,schemaName,thisEntityTable.name,thisEntityTable.alias)
#            if i< len(self.tables):
#                tableLine=tableLine+","
#            script = script + tableLine
#        if initialWhereCondition is None:
#            script = script + "\n%sWHERE (\n" %(indent)
#        else:
#            script = script + "\n%sWHERE %s AND (\n" %(indent, initialWhereCondition)
#
#        i=0
#        for thisEntityJoins in self.joins:
#            i=i+1
#            joinLine="  %s%s" %(indent,thisEntityJoins.condition)
#            if i<len(self.joins):
#                joinLine=joinLine+" AND"
#            if i!=len(self.joins):
#                joinLine=joinLine+"\n"
#            else:
#                joinLine=joinLine+")"
#            script = script + joinLine  
#            
#        return(script)                      

    def hasCtree(self):
        result=False
        for element in self.computedData.elements:
            if element.type=="ctree":
                result = True
        return(result)
    
    def getAllFinalColumns(self):
        allColumns=[]
        for thisEntitiyTable in self.tables:
            for thisEntityColumn in thisEntitiyTable.columns:
                allColumns.append(thisEntityColumn.finalEntityColumn)
        return(allColumns)
        
    
    def getAllFields(self, specification, applyPrefix, includeComputedFields, appLogger, tableRestriction, returnSourceColumnName=False):
        
        allFields = []
        for thisEntitiyTable in self.tables:
            
            for thisRecord in specification.records:
                if (tableRestriction is not None and thisRecord.table==tableRestriction) or (tableRestriction is None and thisRecord.table == thisEntitiyTable.name):
            
                    for thisEntityColumn in thisEntitiyTable.columns:
                        
                        matchFound=False
                        for thisField in thisRecord.fields:
                            if thisField.column is not None:
                                if thisField.column == thisEntityColumn.column is not None:                                    
                                    
                                    if returnSourceColumnName:
                                        targetColumn = thisEntityColumn.column
                                    else:
                                        targetColumn = thisEntityColumn.finalEntityColumn
                                    field = SpecificationRecordField(None, None, label=thisField.label, column=targetColumn, redirectedFromColumn=None, type=thisField.type, array=thisField.array, mandatory=thisField.mandatory, size=thisField.size, default=None, decimalPlaces=thisField.decimalPlaces)
                                    matchFound=True
                                    allFields.append(field)
                                    
                                    
                        if not matchFound:
                            for thisField in thisRecord.additionalFields:
                                if thisField.column is not None:
                                    if thisField.column == thisEntityColumn.column is not None:
                                        if returnSourceColumnName:
                                            targetColumn = thisEntityColumn.column
                                        else:
                                            targetColumn = thisEntityColumn.finalEntityColumn
                                        field = SpecificationRecordField(None, None, label=thisField.label, column=targetColumn, redirectedFromColumn=None, type=thisField.type, array=thisField.array, mandatory=thisField.mandatory, size=thisField.size, default=None, decimalPlaces=thisField.decimalPlaces)
                                        matchFound=True
                                        allFields.append(field)
        
                    if includeComputedFields:
                        allFields.extend(thisRecord.computedData.getAllFields())
            
        if includeComputedFields:
            allFields.extend(self.computedData.getAllFields())
                        
        return(allFields)
    

    def __init__(self, specification, entityTag, settings, appLogger):
        
        self.name=cs.grabAttribute(entityTag,"name")            
        self.label=cs.grabAttribute(entityTag,"label")        
        self.whereClause =cs.grabAttribute(entityTag,"whereClause")
        
        self.defaultVisibility=cs.grabAttribute(entityTag,"defaultVisibility")
        if self.defaultVisibility is not None:
            self.defaultVisibility = int(self.defaultVisibility)
        
        self.defaultSecurity=cs.grabAttribute(entityTag,"defaultSecurity")
        if self.defaultSecurity is not None:
            self.defaultSecurity = int(self.defaultSecurity)

        self.fillFactor=cs.grabAttribute(entityTag,"fillFactor")
        if self.fillFactor is not None:
            self.fillFactor = int(self.fillFactor)  
                        
       
        if appLogger is not None:
            appLogger.debug("%s (%s)" %(self.name, self.label))
            appLogger.debug("")
            appLogger.debug("  defaultVisibility: %s" %(cs.prettyNone(self.defaultVisibility)))
            appLogger.debug("  defaultSecurity  : %s" %(cs.prettyNone(self.defaultSecurity)))
            appLogger.debug("  fillFactor         : "+cs.prettyNone(str(self.fillFactor)))

            appLogger.debug("")


        #Tables
        self.tables=[]
        tablesTag=entityTag.getElementsByTagName("tables")
        if tablesTag.length > 0:
            tablesTag = tablesTag[0]
            tables=tablesTag.getElementsByTagName("table")  
            if appLogger is not None:              
                appLogger.debug("  Tables:")   
            for thisEntityTable in tables:
                newRecord=EntityTableRecord(self, specification, thisEntityTable, appLogger)
                self.tables.append(newRecord)
    
        # Set "final" versions of join columns...
        # (e.g. if a column will be eventually aliased then this is the aliased column name, if not it's the "base" column name.
        for table in self.tables:
            for join in table.joins:                    
                for candidateTable in self.tables:
                    if candidateTable.name == join.foreignTable:                        
                        for candidateColumn in candidateTable.columns:
                            if candidateColumn.column == join.foreignColumn:
                                join.setFinalForeignColumn(candidateColumn.finalEntityColumn)
                                 
#            
#        for tableColumn in table.columns:
#            if self.column == tableColumn.column:
#                self.finalEntityColumn = tableColumn.finalEntityColumn
#            if self.foreignColumn ==  tableColumn.column:
#                self.finalEntityForeignColumn = tableColumn.finalEntityColumn
#
#
#        
#        
        # Calculate primary key...
        #self.primaryKey=["{0}_id".format(self.tables[0].name)]
        #for table in self.tables:
        #    if table.joinType is not None:
        #        if table.joinType=="inner":
        #            self.primaryKey.append("{0}_id".format(table.name))
        #                            
        #if appLogger is not None:
        #    appLogger.debug("")
        #    appLogger.debug("   primaryKey: {0}".format(str(self.primaryKey)))


        #Additional Indexes
#        self.additionalIndexes=[]
#        additionalIndexesTag = entityTag.getElementsByTagName("additionalIndexes")
#        if additionalIndexesTag.length > 0:
#            additionalIndexesTag = additionalIndexesTag[0]
#            indexesTag = additionalIndexesTag.getElementsByTagName("index")
#            
#            for thisIndex in indexesTag:
#                self.additionalIndexes.append(AdditionalIndex(thisIndex))

        #Capture additional indexes
        # (but do it on a "child" basis as the pin tag can have additionalIndexes as well)
        self.additionalIndexes=[]
        for child in entityTag.childNodes:
            if child.localName == "additionalIndexes":
                indexesTag = child.getElementsByTagName("index")                    
                for thisIndex in indexesTag:
                    self.additionalIndexes.append(AdditionalIndex(thisIndex))
        
        
        if appLogger is not None:
            appLogger.debug("  Joins:")
        for table in self.tables:
            table.setJoinCondition(self.tables)
            if appLogger is not None:
                appLogger.debug("    {0}: {1} [{2}]".format(table.name, table.joinCondition, table.joinType))

        # =================================
        computedDataTag = entityTag.getElementsByTagName("computedData")
        self.computedData = calc.CalculatedData("entity", computedDataTag, settings)
        if appLogger is not None:
            self.computedData.debug(appLogger)
            
            
#        self.search = None
#        searchTag = entityTag.getElementsByTagName("search")     
#        if len(searchTag)>0:
#            if appLogger is not None:
#                appLogger.debug("")
#                appLogger.debug("Searches (entity)")
#                appLogger.debug("-----------------")
#                                        
            # allFields = self.getAllFields(specification, False, appLogger)

            #for thisRecord in specification.records:

            #    if thisRecord.table == self.leadTable:                                                
            #        self.search = search.Search(False, searchTag, allFields, thisRecord.primaryKeyColumns, appLogger)
                                            
        if appLogger is not None:
            appLogger.debug("")            
            appLogger.debug("  From clause: %s" %(self.getFromClause("[schema]", True)))
            appLogger.debug("  whereClause      : "+cs.prettyNone(self.whereClause))

            
#    def getLeadTable(self):
#        return list(filter(lambda table: table.name == self.leadTable, self.tables))[0]
    
    def getFromClause(self, schema, additionalOuterJoinRestrictions):
        # Build-up from clause... start with the table that has no join information
        for table in self.tables:
            if table.joinType is None:
                fc = "%s.%s" %(schema, table.name)
                if table.alias is not None:
                    fc = fc + " AS %s" %(table.alias)
                    
        # Now chain the rest of them...
        for table in self.tables:
            if table.joinType is not None:
                fc = fc + " %s join %s.%s" %(table.joinType, schema, table.name)
                if table.alias is not None:
                    fc = fc + " AS %s" %(table.alias)
                    
                fc=fc + " on (%s" %(table.joinCondition)
                if additionalOuterJoinRestrictions:
                    if table.joinType=="left":
                        if table.joinFilter is not None:
                            fc +=" AND (%s)" %(table.joinFilter)
                        fc += " AND (%s.visibility > 10 OR %s.visibility is null)" %(table.alias, table.alias) 
                fc=fc + ")"      
        return(fc)
        
    def getFullFromAndWhereClauses(self, schema, initialWhereClause, additionalOuterJoinRestriction):
        clauses = "FROM " + self.getFromClause(schema, additionalOuterJoinRestriction)
        if initialWhereClause is not None or self.whereClause is not None:
            clauses = clauses + " WHERE "
            if initialWhereClause is not None:
                clauses = clauses + "(%s)" %(initialWhereClause)
                if self.whereClause is not None:
                    clauses = clauses + " AND "                        
            if self.whereClause is not None:
                clauses = clauses + self.whereClause
        return(clauses)
        
        
class EntityTableColumn():
    def __init__(self, specificationRecord, name, alias):
        self.column = str(name)
        if alias is not None:
            self.alias  = str(alias)
            self.finalEntityColumn = self.alias  
        else:
            self.alias = None
            self.finalEntityColumn = self.column  
        
        self.strippedColumnClause=None
        for field in specificationRecord.fields:
            if field.column == name:
                self.strippedColumnClause = field.strippedColumnClause(self.finalEntityColumn, False)
        
        if self.strippedColumnClause is None:
            for field in specificationRecord.additionalFields:
                if field.column == name:
                    self.strippedColumnClause = field.strippedColumnClause(self.finalEntityColumn, False)
             
        if self.strippedColumnClause is None:
            for field in specificationRecord.computedData.getAllFields():
                if field.column == self.column:
                    scc = field.strippedColumnClause(self.finalEntityColumn, False)              
                    self.strippedColumnClause = scc

#        
#        self.name = str(name)
#        if name in recordPrimaryKeyColumns:
#            self.sourcePrefix="pk"
#        else:
#            self.sourcePrefix="w"
#            
#        self.sourceName = "%s_%s" %(self.sourcePrefix,self.name)
#        
#            
#        if name in entityPrimaryKeyColumns:
#            self.entityPrefix="pk"
#        else:
#            self.entityPrefix="w"
#
#        
#        if alias is not None:
#            self.alias  = str(alias)
#            self.finalEntityField=self.alias
#            self.finalEntityColumn = "%s_%s" %(self.entityPrefix,self.alias)
#        else:
#            self.alias  = None
#            self.finalEntityField=self.name
#            self.finalEntityColumn = "%s_%s" %(self.entityPrefix,self.name)

class EntityColumnPair():
    
    
    def setFinalColumn(self, column):
        self.finalColumn = column
        
    def setFinalForeignColumn(self, column):
        self.finalForeignColumn = column
    
    def __init__(self, columnPairTag, table, appLogger):
        self.column = cs.grabAttribute(columnPairTag,"column") 
        self.foreignTable = cs.grabAttribute(columnPairTag,"foreignTable")
        self.foreignColumn = cs.grabAttribute(columnPairTag,"foreignColumn")
                    
        if appLogger is not None:
            appLogger.debug("        {0}.{1} = {2}.{3}".format(table.name, self.column, self.foreignTable, self.foreignColumn))


class EntityTableRecord:

        
    def setJoinCondition(self, entityTables):
        if self.joinType is None:
            self.joinCondition = None
        else:         
            jc=[]
            for join in self.joins:
                for candidateTable in entityTables:
                    if candidateTable.name == join.foreignTable:
                        for candidateColumn in candidateTable.columns:
                            if candidateColumn.column == join.foreignColumn:
                                condition =  "({0}.{1} = {2}.{3})".format(self.alias, join.column,candidateTable.alias,join.foreignColumn )
                                jc.append(condition)
                self.joinCondition = " AND ".join(jc)                        
   
                
        
        
                
    def __init__(self, entityRecord, specification, tableTag, appLogger):

    
        self.entityRecord = entityRecord
        
        self.name=cs.grabAttribute(tableTag,"name")                    
        self.alias=cs.grabAttribute(tableTag,"alias")
        
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("  * %s (alias ='%s')" %(self.name, self.alias))
        
        self.columns=[]
        self.additionalTriggeringColumns=[]
        self.leadTableRefreshTriggeringColumns=[]
        columnsTag=tableTag.getElementsByTagName("columns")
        
        if len(columnsTag)>0: 
            columnsTag = columnsTag [0]
            columns=columnsTag.getElementsByTagName("column")
            
            for thisRecord in specification.records:
                if thisRecord.table==self.name:                                                
            
                    for thisColumn in columns:
                        columnName=str(cs.grabAttribute(thisColumn, "name"))
                        alias=cs.grabAttribute(thisColumn, "alias")

                        entityTableColumn = EntityTableColumn(thisRecord, columnName, alias)
                        self.columns.append(entityTableColumn)
                
            if appLogger is not None:
                appLogger.debug("      Columns:")
            
                for thisColumn in self.columns:   
                    appLogger.debug("        %s [column='%s' alias='%s']" %(thisColumn.finalEntityColumn, thisColumn.column, thisColumn.alias))
                

        leadTableRefreshTriggeringColumnsTag=tableTag.getElementsByTagName("leadTableRefreshTriggeringColumns")
        if len(leadTableRefreshTriggeringColumnsTag)>0: 
            leadTableRefreshTriggeringColumnsTag = leadTableRefreshTriggeringColumnsTag [0]
            columns=leadTableRefreshTriggeringColumnsTag.getElementsByTagName("column")            
            for thisColumn in columns:
                columnName=str(cs.grabAttribute(thisColumn, "name"))
                self.leadTableRefreshTriggeringColumns.append(columnName)

        additionalTriggeringColumnsTag=tableTag.getElementsByTagName("additionalTriggeringColumns")
        if len(additionalTriggeringColumnsTag)>0: 
            additionalTriggeringColumnsTag = additionalTriggeringColumnsTag [0]
            columns=additionalTriggeringColumnsTag.getElementsByTagName("column")            
            for thisColumn in columns:
                columnName=str(cs.grabAttribute(thisColumn, "name"))
                self.additionalTriggeringColumns.append(columnName)

            if appLogger is not None:     
                appLogger.debug("      additionalTriggeringColumns      : %s" %(", ".join(self.additionalTriggeringColumns)))
                appLogger.debug("      leadTableRefreshTriggeringColumns: %s" %(", ".join(self.leadTableRefreshTriggeringColumns)))
        
        self.joinType = None
        self.joinFilter = None
        self.joins = []

        # JOIN
        joinTag=tableTag.getElementsByTagName("join")        
        if len(joinTag)>0: 
            joinTag = joinTag [0]
            self.joinType = cs.grabAttribute(joinTag,"joinType")

            # Default to inner if not specified
            if self.joinType is None:
                self.joinType="inner"

            self.joinFilter = cs.grabAttribute(joinTag,"filter")
            if appLogger is not None:
                appLogger.debug("      joinType      : " + cs.prettyNone(self.joinType))
                appLogger.debug("      joinFilter    : " + cs.prettyNone(self.joinFilter))
                appLogger.debug("      joins: ")
        
            joiningColumnsTag=joinTag.getElementsByTagName("joiningColumns")[0]       
            columnPairs=joiningColumnsTag.getElementsByTagName("columnPair")
            for columnPair in columnPairs:
                self.joins.append(EntityColumnPair(columnPair,  self, appLogger))
                    
           
class EntityJoinRecord:
    def __init__(self, joinTag, appLogger):
        self.condition=cs.grabAttribute(joinTag,"condition")
        self.filter=cs.grabAttribute(joinTag,"filter")
        if appLogger is not None:
            appLogger.debug("    "+self.condition)  



class SpecificationRecordField:

    
    def setValues(self, label, column, redirectedFromColumn, type, mandatory, size, decimalPlaces, default, array, dataitem, dataitemName, description, tags, appLogger):
        self.label = label
        self.column = column
        self.redirectedFromColumn = redirectedFromColumn
        self.type = type
        self.mandatory = mandatory
        self.size = size
        self.decimalPlaces = decimalPlaces
        self.array = array
        self.default = default
        self.dataitem = dataitem
        self.description = description
        self.tags = tags
        
        if dataitem:
            if dataitemName is None:
                self.dataitemName = column
            else:
                self.dataitemName = dataitemName
        else:
            self.dataitemName = None
            
        
        if self.column is not None:
            if appLogger is not None:
                logLine = "  {0} [dataitem={1}]".format(self.columnClause(None), dataitem)
                appLogger.debug(logLine)
        else:
            if appLogger is not None:
                logLine = "    "+cs.prettyNone(self.label)+" (not mapped to a column) [dataitem: {0}={1}]".format(self.dataitemName, self.dataitem)
                appLogger.debug(logLine)
                if len(self.tags) >0:
                    appLogger.debug("      Tags: {0}".format(",".join(self.tags)))
            
        self.columnDataType=None
        self.dumpDataType=None                
                
        if self.type=="text":
            self.columnDataType="character varying"
            self.dumpDataType="text"
            
        elif self.type=="number":
            if self.decimalPlaces is None:

                if self.size >0 and self.size <=4:
                    self.columnDataType="integer"
                    self.dumpDataType="integer"
                    
                elif self.size>4 and self.size<=9:
                    self.columnDataType="integer"
                    self.dumpDataType="integer"
                    
                elif self.size>9:
                    self.columnDataType="bigint"
                    self.dumpDataType="bigint"
            else:
                if decimalPlaces<5:
                    self.columnDataType="decimal"
                    self.dumpDataType="decimal"
                else:
                    self.columnDataType="double precision"
                    self.dumpDataType="double precision"
                
        elif self.type=="date":
            self.columnDataType="date"
            self.dumpDataType="date"
            
        elif self.type=="time":
            self.columnDataType="time with time zone"
            self.dumpDataType="time"
            
        elif self.type=="datetime":
            self.columnDataType="timestamp with time zone"
            self.dumpDataType="timestamp with time zone"   

        elif self.type=="geometry":
            self.columnDataType="geometry"
            self.dumpDataType="geometry"   

        if self.array:
            self.columnDataType=self.columnDataType+"[]"
            self.dumpDataType=self.dumpDataType+"[]"
            
                
    
    def __init__(self, fieldTag, appLogger, label=None, column=None, redirectedFromColumn=None, type=None, array=None, mandatory=None, size=None, default=None, decimalPlaces=None, dataitem=None, dataitemName=None, description=None, tags=None):


        if fieldTag is not None:      
            label = cs.grabAttribute(fieldTag,"label")
            column = cs.grabAttribute(fieldTag,"column")
            redirectedFromColumn = cs.grabAttribute(fieldTag,"redirectedFromColumn")
            
            if column is None:
                column = cs.grabAttribute(fieldTag,"outputColumn")
                            
            type = cs.grabAttribute(fieldTag,"type")
            array = cs.grabAttribute(fieldTag,"array")
            
            if array is None:
                array=False
            else:
                if array=="true":
                    array=True
                else:
                    array=False
            
            mandatoryAttribute = cs.grabAttribute(fieldTag,"mandatory")
            if mandatoryAttribute is not None:
                if (mandatoryAttribute=="True" or mandatoryAttribute=="true"):
                    mandatory=True
                else:
                    mandatory=False
            else:
                mandatory=False
    
            size = cs.grabAttribute(fieldTag,"size")
            if size is not None:
                size = int(size)
    
            decimalPlaces = cs.grabAttribute(fieldTag,"decimalPlaces")
            if decimalPlaces is not None:
                decimalPlaces = int(decimalPlaces)

            dataitem = cs.grabAttribute(fieldTag,"dataitem")                
            if dataitem is None:
                dataitem=False
            else:
                if dataitem=="true":
                    dataitem=True
                else:
                    dataitem=False                

            dataitemName = cs.grabAttribute(fieldTag,"dataitemName")
            
            description = cs.grabAttribute(fieldTag,"description")

            default = cs.grabAttribute(fieldTag,"default")
            
            tags = []
            tagsElement = fieldTag.getElementsByTagName("tags")                                                    
            if len(tagsElement) >0:
                tagsElement = tagsElement[0]
                tagElements = tagsElement.getElementsByTagName("tag")
                for tagElement in tagElements:
                    tagName = cs.grabAttribute(tagElement,"name")
                    tags.append(tagName)
                                                                                                                    
            self.setValues(label, column, redirectedFromColumn, type, mandatory, size, decimalPlaces, default, array, dataitem, dataitemName, description, tags, appLogger)
        
        elif column is not None:
            self.setValues(label, column, redirectedFromColumn, type, mandatory, size, decimalPlaces, default, array, dataitem, dataitemName, description, tags, appLogger)            


        
    def columnClause(self, delimitedPrimaryKey):
        clause="  {0} ".format(self.column)

        if self.type=="text":
            clause=clause+"character varying("+str(self.size)+")"

        elif self.type=="number":

            if self.decimalPlaces is None:

                if self.size >0 and self.size <=4:
                    clause=clause+"integer"
                elif self.size>4 and self.size<=9:
                    clause=clause+"integer"
                elif self.size>9:
                    clause=clause+"bigint"
            else:
                if self.decimalPlaces<5:
                    clause=clause+"decimal("+str(self.size+self.decimalPlaces)+","+str(self.decimalPlaces)+")"
                else:
                    clause=clause+"double precision"

        elif self.type=="date":
            clause=clause+"date"

        elif self.type=="time":
            clause=clause+"time with time zone"

        elif self.type=="datetime":
            clause=clause+"timestamp with time zone"

        elif self.type=="geometry":
            clause=clause+"geometry"
            
        if self.array:
            clause=clause+"[]"

        if self.default is not None:
            clause=clause+" DEFAULT {0}".format(self.default)
            
        if delimitedPrimaryKey is not None:
            if self.column == delimitedPrimaryKey:
                clause=clause+" PRIMARY KEY"
            else:
                if self.mandatory:
                    clause=clause+" NOT NULL"  
        else:
            if self.mandatory:
                clause=clause+" NOT NULL"  

            
        return(clause)


    def strippedColumnClause(self, overriddenColumnName, includeNotNulls):

        if overriddenColumnName is None:
            clause=self.column+" "
        else:
            clause=overriddenColumnName+" "

        if self.type=="text":
            clause=clause+"character varying("+str(self.size)+")"

        elif self.type=="number":

            if self.decimalPlaces is None:

                if self.size >0 and self.size <=4:
                    clause=clause+"integer"
                elif self.size>4 and self.size<=9:
                    clause=clause+"integer"
                elif self.size>9:
                    clause=clause+"bigint"
            else:
                if self.decimalPlaces<5:
                    clause=clause+"decimal("+str(self.size+self.decimalPlaces)+","+str(self.decimalPlaces)+")"
                else:
                    clause=clause+"double precision"

        elif self.type=="date":
            clause=clause+"date"

        elif self.type=="time":
            clause=clause+"time with time zone"

        elif self.type=="datetime":
            clause=clause+"timestamp with time zone"

        elif self.type=="geometry":
            clause=clause+"geometry"

        if self.array:
            clause=clause+"[]"
            
        if self.mandatory and includeNotNulls:
            clause=clause+" NOT NULL"  

        return(clause)






class SpecificationRecord:
    
    def getField(self, columnName):
        return list(filter(lambda field: field.column == columnName, self.getAllMappedFields()))[0]

    def getAllMappedFields(self):
        return itertools.chain(filter(lambda field: field.column is not None, self.fields), self.additionalFields)

    def getAllMappedFieldsIncludingComputed(self):      
        allComputedFields = self.computedData.getAllFields()
 
        r = list(filter(lambda field: field.column is not None, self.fields))
        r.extend(self.additionalFields)
        r.extend(allComputedFields)
        
        return r


    def getInputMappedFields(self):
        return filter(lambda field: field.column is not None, self.fields)

    def hasPrimaryKey(self):
        return len(self.primaryKeyColumns) > 0

    def getPrimaryKeyFields(self):
        return filter(lambda field: field.column in self.primaryKeyColumns, itertools.chain(self.fields, self.additionalFields))
    
    def getAdditionalPinheadFields(self, pin):
        return filter(lambda field: field.column in pin.additionalPinColumns, itertools.chain(self.fields, self.additionalFields))

    def getNativeTable(self):
        return self.table if self.nativeTable is None else self.nativeTable

    def getWorkingTargetSchema(self):
        return "editable" if self.editable else "import" 
    
    def getDestinationTargetSchema(self):
        return None if not self.useful else ("editable" if self.editable else "import")

    def getGeometryFields(self):
        return [field for field in self.fields if field.dumpDataType == "geometry"]

#    def getFinalDestinationInfo(self, entities):
#        info = []
#        
#        for thisPin in self.pins:
#            columnName = "%s_pin_modified" %(thisPin.name)
#            dataType = "timestamp with time zone"
#            tableScript = "%s %s NOT NULL DEFAULT now()" %(columnName, dataType)
#            info.append((columnName, dataType, tableScript))
#        
#        if self.search.enabled:
#            columnName = "search_modified"
#            dataType = "timestamp with time zone"
#            tableScript = "%s %s NOT NULL DEFAULT now()" %(columnName, dataType)
#            info.append((columnName, dataType, tableScript))
#        
#        for thisEntity in entities:
#            for thisTable in thisEntity.tables:
#                if thisTable.name == self.table:
#                    columnName = "mv_%s_modified" %(thisEntity.name)
#                    dataType = "timestamp with time zone"
#                    tableScript = "%s %s NOT NULL DEFAULT now()" %(columnName, dataType)
#                    info.append((columnName, dataType, tableScript))
#
#        columnName = "visibility"
#        dataType = "integer"
#        tableScript = "%s %s" %(columnName, dataType)
#        info.append((columnName, dataType, tableScript))
#        
#        columnName = "security"
#        dataType = "integer"
#        tableScript = "%s %s" %(columnName, dataType)
#        info.append((columnName, dataType, tableScript))
#        
#        return(info)

    def hasCtree(self):
        result=False
        for element in self.computedData.elements:
            if element.type=="ctree":
                result = True
        return(result)
    
    def appendCreateIndexStatement(self,ddl):
        self.createIndexStatements.append(ddl)

    def appendDropIndexStatement(self,ddl):
        self.dropIndexStatements.append(ddl)


    def __init__(self, specificationName, settings, recordTag, appLogger):

        #Init basic values
        self.matches=0
        
        self.label=cs.grabAttribute(recordTag,"label")
        if appLogger is not None:
            appLogger.debug(self.label)

        self.table=cs.grabAttribute(recordTag,"table")
        if appLogger is not None:
            appLogger.debug("  table              : "+cs.prettyNone(self.table))

        self.nativeTable=cs.grabAttribute(recordTag,"nativeTable")
        if appLogger is not None:
            appLogger.debug("  nativeTable        : "+cs.prettyNone(self.nativeTable))


        self.withOids=cs.grabAttribute(recordTag,"withOids")
        if self.withOids is None:
            self.withOids=False
        else:
            if self.withOids=="true":
                self.withOids="true"
            else:
                self.withOids="false"

        self.fillFactor=cs.grabAttribute(recordTag,"fillFactor")
        if self.fillFactor is not None:
            self.fillFactor = int(self.fillFactor)  

                        
        if appLogger is not None:            
            appLogger.debug("  withOids           : "+cs.prettyNone(str(self.withOids)))
            appLogger.debug("  fillFactor         : "+cs.prettyNone(str(self.fillFactor)))

        self.useful=cs.grabAttribute(recordTag,"useful")
        if self.useful is None:
            self.useful=True   
        else:
            if self.useful=="true":
                self.useful=True
            if self.useful=="false":
                self.useful=False
        if appLogger is not None:        
            appLogger.debug("  useful             : "+cs.prettyNone(str(self.useful)))        
    


        self.editable=cs.grabAttribute(recordTag,"editable")
        if self.editable is None:
            self.editable=False 
        else:
            if self.editable=="true":
                self.editable=True
            if self.editable=="false":
                self.editable=False
        if appLogger is not None:        
            appLogger.debug("  editable           : "+cs.prettyNone(str(self.editable)))        

            
            
    
        self.defaultVisibility=cs.grabAttribute(recordTag,"defaultVisibility")
        if self.defaultVisibility is None:
            self.defaultVisibility=70   
            provision="not provided explicitly"
        else:
            self.defaultVisibility = int(self.defaultVisibility)                       
            provision="provided by specification"
        if appLogger is not None:
            appLogger.debug("  defaultVisibility  : "+cs.prettyNone(str(self.defaultVisibility)+" ("+provision+")"))
        
        
        self.defaultSecurity=cs.grabAttribute(recordTag,"defaultSecurity")
        if self.defaultSecurity is None:
            self.defaultSecurity=70   
            provision="not provided explicitly"
        else:
            self.defaultSecurity = int(self.defaultSecurity)                       
            provision="provided by specification"
        if appLogger is not None:
            appLogger.debug("  defaultSecurity    : "+cs.prettyNone(str(self.defaultSecurity)+" ("+provision+")"))
    
        #Capture identification information (if any)
        self.recordRegex = None            
        self.compiledRecordRegex = None
        self.insertRegex = None            
        self.compiledInsertRegex = None
        self.mergeRegex = None            
        self.compiledMergeRegex = None
        self.updateRegex = None            
        self.compiledUpdateRegex = None
        self.deleteRegex = None            
        self.compiledDeleteRegex = None

        identificationTag=recordTag.getElementsByTagName("identification")
        if identificationTag.length > 0:
            identificationTag = identificationTag[0]
            defaultAction=cs.grabAttribute(identificationTag,"defaultAction")
            if defaultAction is not None:
                self.defaultAction = defaultAction
            else:
                self.defaultAction = "INSERT"
                
            self.recordRegex = cs.grabTag(identificationTag,"recordRegex")
            if self.recordRegex is not None:                                 
                self.compiledRecordRegex=re.compile(self.recordRegex)    
            
            self.insertRegex = cs.grabTag(identificationTag,"insertRegex")                                
            if self.insertRegex is not None:
                self.compiledInsertRegex=re.compile(self.insertRegex)    

            self.mergeRegex = cs.grabTag(identificationTag,"mergeRegex")                                
            if self.mergeRegex is not None:
                self.compiledMergeRegex=re.compile(self.mergeRegex)    

            self.updateRegex = cs.grabTag(identificationTag,"updateRegex")                                
            if self.updateRegex is not None:
                self.compiledUpdateRegex=re.compile(self.updateRegex)    

            self.deleteRegex = cs.grabTag(identificationTag,"deleteRegex")                                
            if self.deleteRegex is not None:
                self.compiledDeleteRegex=re.compile(self.deleteRegex)    
            
        else:
            self.defaultAction = 'INSERT'
        if appLogger is not None:  
            appLogger.debug("  recordRegex        : "+cs.prettyNone(self.recordRegex))
            appLogger.debug("  defaultAction      : "+cs.prettyNone(self.defaultAction))
            appLogger.debug("  insertRegex        : "+cs.prettyNone(self.insertRegex))
            appLogger.debug("  updateRegex        : "+cs.prettyNone(self.updateRegex))
            appLogger.debug("  deleteRegex        : "+cs.prettyNone(self.deleteRegex))


        #Capture primary key information (if any)
        self.primaryKeyColumns = []
        primaryKeyTag=recordTag.getElementsByTagName("primaryKey")
        if primaryKeyTag.length > 0:
            primaryKeyTag = primaryKeyTag[0]
            columns=primaryKeyTag.getElementsByTagName("column")
            self.primaryKeyColumns=[]
            for thisColumn in columns:
                self.primaryKeyColumns.append(str(cs.grabAttribute(thisColumn,"name")))
        if appLogger is not None:
            appLogger.debug("  primaryKeyColumns   : "+cs.prettyNone(str(self.primaryKeyColumns)))
       
        #Capture closure tree key information (if any)
        self.ancestorColumn=None
        self.descendantColumn=None
        self.columnSuffix = None       
        
        if appLogger is not None:  
            appLogger.debug("")
            appLogger.debug("  Columns...")
            
        #Capture supplied fields
        self.fields=[]
        fieldsTag = recordTag.getElementsByTagName("suppliedFields")
        if fieldsTag.length > 0:
            fieldsTag=fieldsTag[0]
            allFieldTags = fieldsTag.getElementsByTagName("field")
            for thisFieldTag in allFieldTags:
                newField=SpecificationRecordField(thisFieldTag, appLogger)
                self.fields.append(newField)

        #Calculate primary key indexes
        self.primaryKeyIndexes=None
        if self.hasPrimaryKey():
            self.primaryKeyIndexes=[]
   
            for thisKeyColumn in self.primaryKeyColumns:
                i=0
                for thisField in self.fields:
                    if thisField.column==thisKeyColumn:
                        self.primaryKeyIndexes.append(i);

                    i=i+1
        if appLogger is not None:
            appLogger.debug("  primaryKeyIndexes: "+cs.prettyNone(str(self.primaryKeyIndexes)))
            appLogger.debug("  AdditionalFields:")
            
        #Capture additional fields
        self.additionalFields=[]
        fieldsTag = recordTag.getElementsByTagName("additionalFields")
        if fieldsTag.length > 0:
            fieldsTag=fieldsTag[0]
            allFieldTags = fieldsTag.getElementsByTagName("field")
            for thisFieldTag in allFieldTags:
                newField=SpecificationRecordField(thisFieldTag, appLogger)
                self.additionalFields.append(newField)
        
        if appLogger is not None:
            appLogger.debug("")
            
        #Capture area details
        areaTag = recordTag.getElementsByTagName("area")
        if areaTag.length > 0:
            areaTag=areaTag[0]
            self.areaName=cs.grabAttribute(areaTag,"name")
            self.areaGeometryColumn=cs.grabAttribute(areaTag,"geometryColumn")
            self.areaIdColumn=cs.grabAttribute(areaTag,"idColumn")
            self.areaLabelColumn=cs.grabAttribute(areaTag,"labelColumn")
        else:
            self.areaName=None
            self.areaGeometryColumn=None
            self.areaIdColumn=None
            self.areaLabelColumn=None
        if appLogger is not None:
            appLogger.debug("  area           : [areaName={0} areaGeometryColumn={1} areaIdColumn={2} areaLabelColumn={3}]".format(self.areaName, self.areaGeometryColumn, self.areaIdColumn, self.areaLabelColumn))   


        #Capture additional indexes
        # (but do it on a "child" basis as the pin tag can have additionalIndexes as well)
        self.additionalIndexes=[]
        for child in recordTag.childNodes:
            if child.localName == "additionalIndexes":
                indexesTag = child.getElementsByTagName("index")
                    
                for thisIndex in indexesTag:
                    self.additionalIndexes.append(AdditionalIndex(thisIndex))


        #Capture additional stage indexes            
        self.additionalStageIndexes=[]
        additionalStageIndexesTag = recordTag.getElementsByTagName("additionalStageIndexes")
        if additionalStageIndexesTag.length > 0:
            additionalStageIndexesTag = additionalStageIndexesTag[0]
            indexesTag = additionalStageIndexesTag.getElementsByTagName("index")
            
            for thisIndex in indexesTag:
                self.additionalStageIndexes.append(AdditionalIndex(thisIndex))

        
        if appLogger is not None:        
            appLogger.debug("")
            appLogger.debug("  Pre-calculated function calls...")
        
        # STAGE DML       
        self.stageCall="select * from stage.stage_%s("%(self.table)            
        paramCount = 3
        for thisField in self.fields:
            if thisField.column is not None:
                paramCount = paramCount + 1
        for i in range(paramCount):
            if i>0:
                self.stageCall=self.stageCall+","                
            self.stageCall=self.stageCall+"%s"                            
        self.stageCall=self.stageCall+")"

        # VALIDATE DML   
        paramCount = 0    
        self.validationCall="select * from stage.%s_valid("%(self.table)            
        for thisField in self.fields:
            if thisField.column is not None:
                paramCount = paramCount + 1
        
        for i in range(paramCount):
            if i>0:
                self.validationCall=self.validationCall+","                
            self.validationCall=self.validationCall+"%s"                            
        self.validationCall=self.validationCall+")"        
        
        if appLogger is not None:
            appLogger.debug("    stageCall      : %s" %(self.stageCall))   
            appLogger.debug("    validationCall : %s" %(self.validationCall))
            appLogger.debug("")


            
        #Grab any transformer instructions
        #=================================
        
        self.columnTransforms = []
        transformerTag = recordTag.getElementsByTagName("transformer")
        if len(transformerTag)>0:
            if appLogger is not None:
                appLogger.debug("")
                appLogger.debug("Transformer")
                appLogger.debug("-----------")
            transformerTag = transformerTag[0]
        
            allColumnTransforms = transformerTag.getElementsByTagName("columnTransform")
            for thisColumnTransform in allColumnTransforms:
                newRecord=ColumnTransform(settings.transformFunctions, thisColumnTransform, appLogger)
                self.columnTransforms.append(newRecord)       
                

        # Grab triggering actions
        # =======================
        self.mvModifyingActions = []
        triggeredActionsTag = recordTag.getElementsByTagName("triggeredActions")
        if len(triggeredActionsTag)>0:
            if appLogger is not None:
                appLogger.debug("")
                appLogger.debug("Triggered actions")
                appLogger.debug("-----------------")
            triggeredActionsTag = triggeredActionsTag[0]
            
            setMvModifiedTags = triggeredActionsTag.getElementsByTagName("setMvModified")            
            for thisAction in setMvModifiedTags:
                newAction = MvModifyingAction(thisAction, self, appLogger)
                self.mvModifyingActions.append(newAction)
            

        # =================================
        computedDataTag = recordTag.getElementsByTagName("computedData")
        self.computedData = calc.CalculatedData("table", computedDataTag, settings)
        if appLogger is not None:
            self.computedData.debug(appLogger)

        # =================================
        
        alertsTag = recordTag.getElementsByTagName("alerts")
        self.alerts={}
        if len(alertsTag) >0:
            alertsTag = alertsTag[0]
            for alertTag in alertsTag.childNodes:
                if hasattr(alertTag,"tagName"):
                    enabled = cs.grabAttribute(alertTag,"enabled")
                    if enabled is not None:
                        if enabled in("true","True","TRUE"):
                            schema = alertTag.tagName[:-6]                         
                            self.alerts[schema] = alert.Alert(specificationName, schema, settings, record=self)
                            self.alerts[schema].debug(appLogger)
            
#  
#        
#        alertsEnabled =cs.grabAttribute(recordTag,"alerts")
#        
#        
#        if alertsEnabled is None:
#            alertsEnabled = self.editable
#        else:
#            if alertsEnabled in("true","True","TRUE"):
#                alertsEnabled=True
#            else:
#                alertsEnabled=False
#        self.alerts = alert.Alert(alertsEnabled, settings, record = self)
#        self.alerts.debug(appLogger)
        
class Spec:

    def getEntitiesForTable(self, tableName):
        result = []
        for entity in self.entities:
            for table in entity.tables:
                if tableName == table.name:
                    result.append(entity)
        return result

    def getUsefulRecords(self):
        return [record for record in self.records if record.useful]

    def getUsefulEditableRecords(self):
        return [record for record in self.records if record.useful and record.editable]

    def getIconInfo(self, databaseObjectType, tableName):

        r = None
        
        if databaseObjectType=="table":
            for thisRecord in self.records:
                if thisRecord.table==tableName:
                    search=thisRecord.search

        if databaseObjectType=="view":
            for thisEntity in self.entities:
                if thisEntity.name==tableName:
                    search=thisEntity.search
        if search is not None:
            if search.iconAssembler.pinheadIconGeneratorName is not None:
                for thisRecord in self.records:
                    for thisPin in thisRecord.pins:
                        for thisGenerator in thisPin.iconGenerators:
                            if thisGenerator.name==search.iconAssembler.pinheadIconGeneratorName:
                                r=thisGenerator.mapping
    
            else:
                r = search.iconAssembler.fixedIconName  
        return(r)
        

    def __init__(self, settings, dataSpecificationName, sendToLog):
        
        if sendToLog:
            appLogger = settings.appLogger
        else:
            appLogger = None
        
        if appLogger is not None:
            appLogger.debug("")
            appLogger.debug("=====================")
            appLogger.debug("Specification details")
            appLogger.debug("=====================")
            appLogger.debug("")
        if dataSpecificationName is not None:
            
            self.dataSpecificationFile = os.path.join(settings.paths["repository"], "specifications", dataSpecificationName, "specification.xml")  
            if appLogger is not None:          
                appLogger.debug("  dataSpecificationFile      : " + self.dataSpecificationFile)
    
            xmldoc = xml.dom.minidom.parse(self.dataSpecificationFile)
            specificationTag = xmldoc.getElementsByTagName("specification")[0]
    
            self.name = dataSpecificationName
            if appLogger is not None:
                appLogger.debug("  name                       : "+cs.prettyNone(self.name))
    
            self.label=cs.grabAttribute(specificationTag,"label")
            if appLogger is not None:
                appLogger.debug("  label                      : "+cs.prettyNone(self.label))
    
            self.vendor=cs.grabAttribute(specificationTag,"vendor")
            if appLogger is not None:
                appLogger.debug("  vendor                     : "+cs.prettyNone(self.vendor))
    
            self.version=cs.grabAttribute(specificationTag,"version")
            if appLogger is not None:
                appLogger.debug("  version                    : "+cs.prettyNone(self.version))

            self.progressReportFrequency=cs.grabAttribute(specificationTag,"progressReportFrequency")
            if self.progressReportFrequency is not None:
                self.progressReportFrequency =int(self.progressReportFrequency)
                if appLogger is not None:
                    appLogger.debug("  progressReportFrequency    : "+str(self.progressReportFrequency))
            else:
                if appLogger is not None:
                    appLogger.debug("  progressReportFrequency    : Never") 



            # Get staging config
            # =================
            self.dedicatedStagingAreaName = None
            self.autoRemoveStageDuplicates = False
            self.stageDuplicateColumns = None
            
            stageConfigTag = specificationTag.getElementsByTagName("stageConfig")
            if len(stageConfigTag) > 0:
                stageConfigTag = stageConfigTag[0]
                        
                self.dedicatedStagingAreaName=cs.grabAttribute(stageConfigTag,"dedicatedSchemaName")
                if appLogger is not None:
                    appLogger.debug("  dedicatedStagingAreaName         : "+cs.prettyNone(self.dedicatedStagingAreaName))

                duplicateRemovalTag = specificationTag.getElementsByTagName("duplicateRemoval")
                if len(duplicateRemovalTag) > 0:
                    duplicateRemovalTag = duplicateRemovalTag[0]

                    self.autoRemoveStageDuplicates=cs.grabAttribute(duplicateRemovalTag,"enabled")
            
                    if self.autoRemoveStageDuplicates is not None:
                        if self.autoRemoveStageDuplicates == "true":
                            self.autoRemoveStageDuplicates = True
                        else:
                            self.autoRemoveStageDuplicates = False                    
                    else:
                        self.autoRemoveStageDuplicates = False

                
                    distinctColumnsTag = stageConfigTag.getElementsByTagName("distinctColumns")
                    if len(distinctColumnsTag) > 0:
                        distinctColumnsTag = distinctColumnsTag[0]
                        columnTags = distinctColumnsTag.getElementsByTagName("column")
                        parts=[]
                        for thisColumn in columnTags:
                            columnName=cs.grabAttribute(thisColumn,"name")
                            parts.append(columnName)
                            self.stageDuplicateColumns = parts
                                                
            if appLogger is not None:
                appLogger.debug("  autoRemoveStageDuplicates  : %s" %(cs.prettyNone(self.autoRemoveStageDuplicates)))

            

            # Get source information
            # ======================
            self.sourceType=None
            self.qualifier = None
            
            sourceTag = specificationTag.getElementsByTagName("source")        
            if sourceTag.length>0:
                sourceTag = sourceTag[0]
        
                self.fileWildcard=cs.grabAttribute(sourceTag,"fileWildcard")
                if appLogger is not None:
                    appLogger.debug("  fileWildcard               : "+cs.prettyNone(self.fileWildcard))
                
                csvFormatTag = sourceTag.getElementsByTagName("csvFormat")
                
                if csvFormatTag.length>0:  

                    self.sourceType="csv"         
                    csvFormatTag = csvFormatTag[0]
            
                    self.delimiter=cs.grabAttribute(csvFormatTag,"delimiter")
                    if appLogger is not None:
                        appLogger.debug("  delimiter                  : "+cs.prettyNone(self.delimiter))
            
                    self.qualifier=cs.grabAttribute(csvFormatTag,"qualifier")
                    if appLogger is not None:
                        appLogger.debug("  qualifier                  : "+cs.prettyNone(self.qualifier))
            
                    self.dateFormat=cs.grabAttribute(csvFormatTag,"dateFormat")
                    if appLogger is not None:
                        appLogger.debug("  dateFormat                 : "+cs.prettyNone(self.dateFormat))
            
                    self.timeFormat=cs.grabAttribute(csvFormatTag,"timeFormat")
                    if appLogger is not None:
                        appLogger.debug("  timeFormat                 : "+cs.prettyNone(self.timeFormat))
            
                    self.dateTimeFormat=cs.grabAttribute(csvFormatTag,"dateTimeFormat")
                    if appLogger is not None:
                        appLogger.debug("  dateTimeFormat             : "+cs.prettyNone(self.dateTimeFormat))
                                
                    self.qualifiedDates=cs.grabAttribute(csvFormatTag,"qualifiedDates")
                    if self.qualifiedDates is not None:
                        if self.qualifiedDates =="true":
                            self.qualifiedDates = True
                        else:
                            self.qualifiedDates = False
                    else:
                        self.qualifiedDates = False  
                    if appLogger is not None:                      
                        appLogger.debug("  qualifiedDates             : "+cs.prettyNone(str(self.qualifiedDates)))
        
                    self.qualifiedTimes=cs.grabAttribute(csvFormatTag,"qualifiedTimes")
                    if self.qualifiedTimes is not None:
                        if self.qualifiedTimes =="true":
                            self.qualifiedTimes = True
                        else:
                            self.qualifiedTimes = False
                    else:
                        self.qualifiedTimes = False
                    if appLogger is not None:
                        appLogger.debug("  qualifiedTimes             : "+cs.prettyNone(str(self.qualifiedTimes)))
        
                    self.qualifiedDateTimes=cs.grabAttribute(csvFormatTag,"qualifiedDateTimes")
                    if self.qualifiedDateTimes is not None:
                        if self.qualifiedDateTimes =="true":
                            self.qualifiedDateTimes = True
                        else:
                            self.qualifiedDateTimes = False
                    else:
                        self.qualifiedDateTimes = False
                    if appLogger is not None:
                        appLogger.debug("  qualifiedDateTimes         : "+cs.prettyNone(str(self.qualifiedDateTimes)))
        
            
                    self.startAtLine=cs.grabAttribute(csvFormatTag,"startAtLine")
            
                    if self.startAtLine is not None:
                        self.startAtLine=int(self.startAtLine)
                        if appLogger is not None:
                            appLogger.debug("  startAtLine                : "+cs.prettyNone(str(self.startAtLine))+" (provided by specification)")
                    else:
                        self.startAtLine=1
                        if appLogger is not None:
                            appLogger.debug("  startAtLine             : "+cs.prettyNone(str(self.startAtLine))+" (defaulted as not in specification)")
                            
                    self.encoding=cs.grabAttribute(csvFormatTag,"encoding")
                    if appLogger is not None:
                        appLogger.debug("  encoding                   : "+cs.prettyNone(self.encoding))

                else:

                    externalLoaderTag = sourceTag.getElementsByTagName("externalLoader")                
                    if externalLoaderTag.length>0:
                        self.sourceType="external"
                        externalLoaderTag=externalLoaderTag[0]
    
                        self.externalLoaderName=cs.grabAttribute(externalLoaderTag,"name")
                        
                        if appLogger is not None:
                            appLogger.debug("  externalLoaderName          : "+cs.prettyNone(self.externalLoaderName))

                        self.externalLoaderProfile=cs.grabAttribute(externalLoaderTag,"profile")
                        if appLogger is not None:
                            appLogger.debug("  externalLoaderProfile          : "+cs.prettyNone(self.externalLoaderProfile))
                            
                        self.externalLoaderVariables={}
                        variablesTag = externalLoaderTag.getElementsByTagName("supplementalVariables")                
                        

                        if variablesTag.length>0:
                            variablesTag=variablesTag[0]
                            allVariables = variablesTag.getElementsByTagName("variable")
                            for thisVariable in allVariables:
                                variableName = cs.grabAttribute(thisVariable,"name")
                                variableValue = cs.grabAttribute(thisVariable,"value")
                                self.externalLoaderVariables[variableName] = variableValue

            if appLogger is not None:
                appLogger.debug("")
                appLogger.debug("  sourceType                 : "+cs.prettyNone(self.sourceType))
                appLogger.debug("")
    
    
            #Register identification tag details (if any)
        
            self.fullRegex= None
            self.compiledFullRegex= None
            self.changeRegex= None
            self.compiledChangeRegex = None
            identificationTag = specificationTag.getElementsByTagName("identification")
        
            if identificationTag.length>0:
                identificationTag = identificationTag[0]
            
                self.fullRegex=cs.grabTag(identificationTag,"fullRegex")
                if self.fullRegex is not None:
                    self.compiledFullRegex=re.compile(self.fullRegex)

                self.changeRegex=cs.grabTag(identificationTag,"changeRegex")
                if self.changeRegex is not None:
                    self.compiledChangeRegex=re.compile(self.changeRegex)
            if appLogger is not None:
                appLogger.debug("  Indentification")            
                appLogger.debug("    fullRegex                : "+cs.prettyNone(self.fullRegex))                        
                appLogger.debug("    changeRegex              : "+cs.prettyNone(self.changeRegex))                        



            # Grab behaviours and default if not provided.
            dmlBehaviourSet=False
            behaviourTag = specificationTag.getElementsByTagName("behaviour")
            if behaviourTag.length>0:
                behaviourTag = behaviourTag [0]
                dmlBehaviourTag = behaviourTag.getElementsByTagName("dml")
                if dmlBehaviourTag.length>0:
                    dmlBehaviourTag = dmlBehaviourTag [0]
                    self.whenDuplicatePrimaryKeyBehaviour = cs.grabAttribute(dmlBehaviourTag,"whenDuplicatePrimaryKey")
                    self.whenNoDataFoundBehaviour = cs.grabAttribute(dmlBehaviourTag,"whenNoDataFound")
                    dmlBehaviourSet=True
            if appLogger is not None:
                appLogger.debug("")
                appLogger.debug("  Behaviour")
            
            if not dmlBehaviourSet:
                self.whenDuplicatePrimaryKeyBehaviour = settings.env["defaultDuplicateKeyBehaviour"]
                self.whenNoDataFoundBehaviour = settings.env["defaultNoDataFoundBehaviour"]
                if appLogger is not None:
                    appLogger.debug("  (taking defaults)")
                    
            if appLogger is not None:
                appLogger.debug("    whenDuplicatePrimaryKeyBehaviour  : {0}".format(self.whenDuplicatePrimaryKeyBehaviour))
                appLogger.debug("    whenNoDataFoundBehaviour          : {0}".format(self.whenNoDataFoundBehaviour))
                
            #Init counters
            self.fileCount=None
            self.lineCount=None
    
            #Build a simple list of record objects
    
            self.records=[]
            allRecordTags = specificationTag.getElementsByTagName("record")
            
            if appLogger is not None:
                appLogger.debug("")
                appLogger.debug("Record Details")
                appLogger.debug("--------------")
                appLogger.debug("")
                
            for thisRecordTag in allRecordTags:
                newRecord=SpecificationRecord(self.name, settings, thisRecordTag, appLogger)
                self.records.append(newRecord)
    
            #Build a simple list of entity objects
            self.entities=[]
            allEntityTags = specificationTag.getElementsByTagName("entity")            
            if appLogger is not None:        
                appLogger.debug("")
                appLogger.debug("Entity Details")
                appLogger.debug("--------------")
            for thisEntityTag in allEntityTags:
                newRecord=EntityRecord(self, thisEntityTag, settings, appLogger)
                self.entities.append(newRecord)
    
    
            # Grab all optionsets
            self.optionSets={}
            allOptionSetTags = specificationTag.getElementsByTagName("optionSet")            
            if appLogger is not None:        
                appLogger.debug("")
                appLogger.debug("Option-Set Details")
                appLogger.debug("------------------")
            for optionSetTag in allOptionSetTags:
                newOptionSet=optionsets.OptionSet(optionSetTag)
                if appLogger is not None:
                    newOptionSet.debug(appLogger)
                self.optionSets[newOptionSet.name] = newOptionSet
            

#            moduleFilename = cs.getChimpScriptFilenameToUse(settings.repositoryPath, ["specification files", dataSpecification, "py", "search formatting"], "%s_search_formatter.py" %(tableName))
 #           module = imp.load_source("%s_search_formatter.py" %(tableName), moduleFilename)
 #           defaultFunctions = module.DefaultSearchProcessor()

            
            
            #Finish up
            xmldoc.unlink()
            if appLogger is not None:
                appLogger.debug("")
            
    