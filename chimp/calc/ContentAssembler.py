'''
Created on 4 Mar 2012

@author: Tim.Needham
'''

import cs


class ContentElement:
    
    def __init__(self, elementTag):
        self.prefix = cs.grabAttribute(elementTag, "prefix")
        self.column = cs.grabAttribute(elementTag, "column")
        self.suffix = cs.grabAttribute(elementTag, "suffix")

    def debug(self, appLogger):
        appLogger.debug("        [{0}] [{1}] [{2}]".format(self.prefix, self.column, self.suffix))
        
        
class ContentAssembler(object):

    def __init__(self, contentAssemblerTag):
        
        self.contentElements = []
        
        if len(contentAssemblerTag) > 0:
            contentAssemblerTag = contentAssemblerTag [0]        
            self.header = cs.grabAttribute(contentAssemblerTag, "header")
            self.delimiter = cs.grabAttribute(contentAssemblerTag, "delimiter")
            self.footer = cs.grabAttribute(contentAssemblerTag, "footer")
            self.format = cs.grabAttribute(contentAssemblerTag, "format")
        
            contentTag = contentAssemblerTag.getElementsByTagName("content")
            if len(contentTag) >0:
                contentTag = contentTag [0]
                for element in contentTag.getElementsByTagName("contentElement"):
                    self.contentElements.append(ContentElement(element))
                                        
        else:
            self.header = None
            self.footer = None
       
    def debug(self, appLogger):
        appLogger.debug("      contentAssembler")
        appLogger.debug("        header: {0}".format(self.header))        
        for element in self.contentElements:
            element.debug(appLogger)
        appLogger.debug("        footer: {0}".format(self.footer))


    def getScript(self,source, specification=None):
        
        
        if len(self.contentElements)==0:
            
            # No content
            script = "\t\tr=None\n"
            script += "\t\treturn(r)"
            
            # Simple return
        elif len(self.contentElements)==1:
            script = '\t\tr=data["{0}"]\n'.format(self.contentElements[0].column)
            script += "\t\treturn(r)"
            
        else:
            # Delimited multiple-parts
            script = "\t\tparts=[]\n"
            for element in self.contentElements:
                script += '\t\tif data["{0}"] is not None:\n'.format(element.column)

                if element.prefix is None and element.suffix is None:
                    script += '\t\t\tparts.append(str(data["{0}"]))\n'.format(element.column)
                else:
                    #TODO: Make this work with prefix/suffix
                    script += '\t\t\tparts.append(str(data["{0}"]))\n'.format(element.column)
                    
                    
            script += ("\t\tif len(parts)>=0:\n"
                       '\t\t\tr="{0}".join(parts)\n'
                       '\t\telse:\n'
                       '\t\t\tr=None\n'
                       '\t\treturn(r)\n\n'.format(self.delimiter))
                            
        return(script)


    def getPinScript(self, outputColumn):
        script=""

        if len(self.contentElements)==1:
            if self.format is None:
                script='\t\tr.append(data["{0}"]) # --> {1}\n'.format(self.contentElements[0].column, outputColumn)
            else:
                script='\t\tr.append("{0}".format(data["{1}"])) # --> {2}\n'.format(self.format, self.contentElements[0].column, outputColumn)
        else:
            if self.delimiter is not None:
                params = ", ".join(map(lambda e:'data["{0}"]'.format(e.column), self.contentElements))
                script += ('\n\t\tl=[{0}]\n'
                           '\t\tl=filter(lambda x:not(x is None),l)\n').format(params)                                                    
                script +='\t\tr.append("{0}".join(map(lambda x:str(x),l))) # --> {1}\n\n'.format(self.delimiter, outputColumn)
            elif self.format is not None:
                params = ", ".join(map(lambda e:'data["{0}"]'.format(e.column), self.contentElements))                    
                script='\t\tr.append("{0}".format({1})) # --> {2}\n'.format(self.format, params, outputColumn)
        return(script)