import cs

def getClosureScript(specification):

    script = ""
    for thisRecord in specification.records:
        if thisRecord.parentKeyColumns is not None:
            if len(thisRecord.parentKeyColumns)>0:

                script = script + "CREATE TABLE ctree.%s_%s_closure (\n" %(thisRecord.table,  cs.delimitedStringList(thisRecord.primaryKeyColumns,"_"))
                
                for thisColumn in thisRecord.primaryKeyColumns:
                    for thisField in thisRecord.fields:
                        if thisColumn == thisField.column:
                            script = script + "  parent_%s %s,\n" %(thisColumn, thisField.strippedColumnClause(None, True))

                for thisColumn in thisRecord.primaryKeyColumns:
                    for thisField in thisRecord.fields:
                        if thisColumn == thisField.column:
                            script = script + "  child_%s %s,\n" %(thisColumn, thisField.strippedColumnClause(None, True))
                
                script = script + "  depth smallint NOT NULL);\n\n" 
            
    return(script)   