
class SpecificationScriptBuilder:
    def __init__(self, specification):
        self.specification = specification
        
    def getStageTransformScript(self, record):                        
        script = "def transformSuppliedValues(dbCursor, data):\n"
        i = 0
        for field in record.fields:
            if field.column is not None:
                script += "\t# [{0}] = {1}\n".format(i, field.column)
                i += 1
                    
        return script + "\treturn(data)\n\n"
    
    def getStorageTransformScript(self, record, schemaName):
        script = "def transformSuppliedValues(dbCursor, data):\n"                
        script = script + "\t# Columns:\n"
        for field in record.fields:
            if field.column is not None:
                script = script + "\t#  %s\n" %(field.column)
        
        for field in record.additionalFields:
            if schemaName == "import":
                script = script + "\tdata.append(None) # %s\n" %(field.column)
            else:
                script = script + "\t#  %s\n" %(field.column)
        
        if schemaName == record.getDestinationTargetSchema():
            script = script + "\tdata.append(None) # visibility\n"
            script = script + "\tdata.append(None) # security\n"
            
        return script + "\treturn(data)\n\n"

