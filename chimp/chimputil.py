'''
Created on 8 Dec 2011

@author: Ryan Pickett
'''
import collections
import re

BuildScriptResult = collections.namedtuple("BuildScriptResult", 
                                           ["filename", "errorsFound", "warningsFound"]) 

def title(s):
    if s is not None:
        r =  re.sub(r"[A-Za-z]+('[A-Za-z]+)?", lambda mo: mo.group(0)[0].upper() +mo.group(0)[1:].lower(),s)
    else:
        r = None
    return(r)
    

def confirm(prompt = "Are you sure?"):
    s = None
    while s not in ["", "n", "N", "y", "Y"]:
        s = input("{0} [n]|y ".format(prompt))
    
    return s in ["y", "Y"]


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
        s=s.strip()
    return(s)

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
