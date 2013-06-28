
import collections
import re


#
#  URN Encoder/Decoder
#
#  Author Paula Thomas <paula.thomas@wmfs.net>
#
class UrnEncoderDecoder:        
    
    """Python class which converts unique reference numbers to Base31 encoded strings """
        
    #
    # encode a URN
    def encode(urn, initializationVector):
        ALPHABET = "0123456789zbcdyfghxjklmnwpqrstv"
        BASE = len(ALPHABET)
        #
        # convert from string to number
        n = int(urn)
        if n == 0:
            return ALPHABET[0]

        #
        # add initialisation vector to make it harder to pre-guess
        # result of encoding
        n = n + initializationVector
    
        # We're only dealing with nonnegative integers.
        if n < 0:
            raise Exception() # Raise a better exception than this in real life.

        result = ""

        #
        # now encode..
        while (n > 0):
            result = ALPHABET[n % BASE] + result           
            n /= BASE
            n = int(n)

        #
        # return result as upper-case string
        return result.upper()

    #
    # decode a string
    
    def decode(originalEncodedValue, initializationVector):
        ALPHABET = "0123456789zbcdyfghxjklmnwpqrstv"
        BASE = len(ALPHABET)
        #
        # force to lower case prior to decoding
        encoded = originalEncodedValue.lower()
        
        result = 0

        #
        # decode
        for i in range(len(encoded)):
            place_value = ALPHABET.index(encoded[i])
            result += place_value * (BASE ** (len(encoded) - i - 1))

        #
        # subtract initialisation vector and return as string
        return result - initializationVector


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
