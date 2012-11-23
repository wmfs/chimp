#from calc.SolrDocument import CapabilityInput
#import calc.SolrDocument

SHARED_SCHEMA = "shared"
CALC_SCHEMA = "calc"
SOLR_SCHEMA = "solr"

def getHeader(capability):
    script="\n        def {0}Formatter():\n".format(capability.name)
    return(script)

def getSource(capability, chimpField, inputIndex, formatting=True):
        
    column=capability.inputs[inputIndex].column
    optionSetName=capability.inputs[inputIndex].optionSetName
    optionSetColumn=capability.inputs[inputIndex].optionSetColumn
    constant=capability.inputs[inputIndex].constant
    
    if optionSetName is not None and optionSetColumn is not None:
        r = 'sourceRow["{0}_{1}"]'.format(optionSetName, optionSetColumn)

    elif column is not None:            
        r = 'sourceRow["{0}"]'.format(column)
                    
    elif constant is not None:
        if chimpField.type=="text":
            r = '"{0}"'.format(constant)
        elif chimpField.type=="number":                
            r = '{0}'.format(constant)
    if formatting:
        r= capability.inputs[inputIndex].wrapReturnValueInFormatting(r)    
    return(r)

class SolrAssemblers():

    def alphaNumericSortAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        script = getHeader(capability)         
        script += ("            components = tools.getOrderByComponents(tools.simpleText(self.label),50)\n"
                   "            self.orderNumberic1 = components[0]\n"            
                   "            self.orderAlpha1 = components[1]\n"
                   "            self.orderNumberic2 = components[2]\n"            
                   "            self.orderAlpha2 = components[3]\n"
                   "            self.orderNumberic3 = components[4]\n"            
                   "            self.orderAlpha3 = components[5]\n"
                   "            self.orderNumberic4 = components[6]\n"            
                   "            self.orderAlpha4 = components[7]\n")    
        return(script)

    def zoneAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        l=len(capability.inputs)

        if l==1:
            # Single input, use default assembler...
            script = self.defaultAssembler(documentName, capability, allFields, defaultVisibility, defaultSecurity, srid)
        else:
            script = getHeader(capability)            
            if l==0:
                script+= "            None\n"
            else:
                # Incoming pairs
                script+= ("            def getZone(tempX, tempY):\n"
                          "                sql = 'select {0}.get_zone(%s,%s)'\n"
                          "                dbCursor.execute(sql,(tempX,tempY))\n"
                          "                r = dbCursor.fetchone()\n"
                          "                if r is not None:\n"
                          "                    r = r[0]\n"
                          "                else:\n"
                          "                    r = None\n"
                          "                return(r)\n").format(SHARED_SCHEMA)

                # Single pair
                if l==2:
                    for field in allFields:
                        if field.name=="zone":
                            chimpField = field.chimpField                    
                    script += "            self.zone = getZone({0},{1}\n)".format(getSource(capability, chimpField, 0), getSource(capability, chimpField, 1))

                else:
                    #Multiple pairs
                    for field in allFields:
                        if field.name=="zone":
                            chimpField = field.chimpField
                    script += "            zones=[]\n"
                    
                    i=0
                    while i<l-1:                    
                        script+="            zones.append(getZone({0},{1}))\n".format(getSource(capability, chimpField, i),getSource(capability, chimpField, i+1))
                        i+=2
                    script += "            self.zone = min(zones)\n"

                
                # Grab x,y pairs
                # --------------
                
        return(script)   
        
    

    def multipleDocumentTypesAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        if len(capability.inputs)>0:
            script = self.defaultAssembler(capability, allFields)
        else:
            script = getHeader(capability)
            script += '            self.documentType = "{0}"\n'.format(documentName)
        return(script)

    def securityAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        if len(capability.inputs)==1:
            script = self.defaultAssembler(documentName, capability, allFields, defaultVisibility, defaultSecurity, srid)
        else:
            script = getHeader(capability)
            script += ('            s = sourceRow["security"]\n'
                       '            if s is None:\n'
                       '                self.security = {0}\n'
                       '            else:\n'
                       '                self.security = s\n').format(defaultSecurity)
        return(script)


    def ctreeAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        if len(capability.inputs)>1:
            script = getHeader(capability)

            # Grab depth:
            column = capability.inputs[0].column
            if column is not None:
                source = column
            else:
                source = capability.inputs[0].constant                
            script += '            self.depth = sourceRow["{0}"]\n'.format(source)

            # Grab root_ancestor:
            column = capability.inputs[1].column
            if column is not None:
                source = column
            else:
                source = capability.inputs[1].constant                
            script += '            self.root_ancestor = sourceRow["{0}"]\n'.format(source)


        else:
            script = getHeader(capability)
            script += "            None\n"
        return(script)
    

    def visibilityAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        if len(capability.inputs)==1:
            script = self.defaultAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid)
        else:
            script = getHeader(capability)
            script += ('            v = sourceRow["visibility"]\n'
                       '            if v is None:\n'
                       '                self.visibility = {0}\n'
                       '            else:\n'
                       '                self.visibility = v\n').format(defaultVisibility)
        return(script)

    def spatialAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        script = getHeader(capability)
        inputCount = len(capability.inputs)
        if inputCount==2:
    
            script += '            if sourceRow["{0}"] is not None and sourceRow["{1}"] is not None:\n'.format(capability.inputs[0].column, capability.inputs[1].column)
            
            # Grab X:
            column = capability.inputs[0].column
            if column is not None:
                source = column
            else:
                source = capability.inputs[0].constant                
            script += '                self.x = int(sourceRow["{0}"])\n'.format(source)
            
            # Grab Y: 
            column = capability.inputs[1].column
            if column is not None:
                source = column
            else:
                source = capability.inputs[0].constant                  
            script += ('                self.y = int(sourceRow["{0}"])\n\n'
                       "                sql = 'select shared.convert_coordinate_to_lat_lon(%s, %s, %s)'\n"
                       "                dbCursor.execute(sql, (self.x, self.y, {1}))\n"
                       "                result=dbCursor.fetchone()\n"
                       "                self.lat_lon = result[0]\n\n"
                       "                if self.lat_lon is not None:\n"
                       '                    s = self.lat_lon.split(",")\n'
                       "                    if len(s)==2:\n"
                       "                        self.latitude=float(s[0])\n"
                       "                        self.longitude=float(s[1])\n").format(source,srid)
        else:
            script += "            None\n"
        return(script)
    
    def containerDocumentAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        script = getHeader(capability)
        inputCount = len(capability.inputs)
        
        if inputCount==2:
            typeField = None
            keyField = None
            for field in allFields:
                #print("{0} - {1}".format(field.capability, capability.name))
                if field.capability == capability.name:
                    if typeField is None:
                        typeField = field
                    elif keyField is None:
                        keyField =field

    
            formattedTypeSource = getSource(capability, typeField.chimpField, 0)
            unformattedTypeSource = getSource(capability, typeField.chimpField, 0, formatting=False)
            formattedKeySource = getSource(capability, keyField.chimpField, 1)
            unformattedKeySource = getSource(capability, keyField.chimpField, 1, formatting=False)

            script += ('            if {0} is not None and {1} is not None:\n'
                       '                self.parentDocumentType = {2}\n'
                       '                self.parentDocumentKey = {3}\n').format(unformattedTypeSource, unformattedKeySource, formattedTypeSource, formattedKeySource)
            
        else:
            script += "            None\n"
        return(script)

    
    def defaultAssembler(self, documentName, capability, allFields, defaultVisibility, defaultSecurity, srid):
        script = getHeader(capability)
        inputCount = len(capability.inputs)
        if inputCount==0:
            script += "            None\n"
        else:
            
            # Count how many fields there are for this capability
            fieldCount = 0
            for field in allFields:
                if field.capability == capability.name:
                    if fieldCount==0:
                        firstCapabilityField = field
                    fieldCount += 1

            # The inputs balance the fields, so a simple mapping...               
            if inputCount==fieldCount:
                for field in allFields:
                    i=0
                    if field.capability ==capability.name:
                        rightSide = getSource(capability, field.chimpField, i)
                        i += 1
                        if capability.format is None:
                            script += "            self.{0} = {1}\n".format(field.variable, rightSide)
                        else:
                            script += '            self.{0} = "{1}".format({2})\n'.format(firstCapabilityField.variable,capability.format, rightSide)

                        

                        
            # Do multiple strings need concatenating somehow?            
            elif fieldCount ==1 and inputCount>1 and firstCapabilityField.chimpField.type=="text":
                if capability.delimiter is not None:
                    script += "            l = []\n"
                    i=0
                    for input in capability.inputs:
                        unformatted = getSource(capability, firstCapabilityField.chimpField, i, formatting=False)
                        script += "            if {0} is not None:\n".format(unformatted)
                        formatted = getSource(capability, firstCapabilityField.chimpField, i)
                        script += "                l.append({0})\n".format(formatted)
                        i += 1
                    script += '            self.{0} = "{1}".join(l)\n'.format(firstCapabilityField.variable, capability.delimiter)    
                
                elif capability.format is not None:
                    inputList = []
                    i=0
                    for input in capability.inputs:
                        rightSide = getSource(capability, firstCapabilityField.chimpField, i)
                        inputList.append(rightSide)
                        i += 1
                    
                    script += '            self.{0} = "{1}".format({2})\n'.format(firstCapabilityField.variable,capability.format.replace('"',"\""), ", ".join(inputList))
                               


        return(script)
    