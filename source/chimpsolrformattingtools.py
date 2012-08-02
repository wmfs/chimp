import re
'''
Created on 4 May 2012

@author: Tim.Needham
'''

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

def getOrderByComponents(s, maxStringLength):    
    components=[]
    components.append(99999)    
    components.append('ZZZZZZ')
    components.append(99999)
    components.append('ZZZZZZ')
    components.append(99999)
    components.append('ZZZZZZ')
    components.append(99999)
    components.append('ZZZZZZ')
    split = re.split('([0-9]+)', s)
    
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
                    number=int(numberString)
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
