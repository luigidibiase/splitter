# -*- coding: utf-8 -*-
"""
Created on Wed May 11 17:02:50 2022

@author: luigi
"""

import datetime
import uuid

#-------------------------------------------------------------------#
# header47
#-------------------------------------------------------------------#

def header47(string):
    
    '''
    Take a string (header) as input and return two strings which are the \n
    the results of a split with sep='|'.                                 \n
    String: string representing the header.
    '''
    
    header_split = string.split(sep='|')
    header_0, header_1 = header_split
    
    header_1 = header_1[0:27] + header_1[33:39] + header_1[49:63]    
    
    return header_0, header_1


#-------------------------------------------------------------------#
# footer
#-------------------------------------------------------------------#

def footer(string):
    
    '''
    Take a string (footer) as input and return two strings which are the \n
    the results of a split with sep='|'.                                 \n
    String: string representing the header.
    '''
    
    footer_split = string.split(sep='|')
    footer_0, footer_1 = footer_split
    
    footer_1 = footer_1[:27] + footer_1[33:39] + footer_1[49:] 
    
    return footer_0, footer_1



#-------------------------------------------------------------------#
# recSplit 
#-------------------------------------------------------------------#

def recSplit(rec):
    
    '''
    Take a string (record) as input and return two strings which are the \n
    the results of a split with sep='|'.                                 \n
    rec: string representing the record.
    '''

    split = rec.split(sep='|')
    split_1, split_2 = split
    
    return split_1, split_2


#-------------------------------------------------------------------#
# split_fields
#-------------------------------------------------------------------#

def split_fields(string, positions):
    
    '''
    Take a string and return a dictionary subsetting the string according to \n
    the values of positions.                                                 \n   
    string: string value (the record or header).                             \n
    positions = a list with the positions to take as values for the slicing.
    '''
    
    fields = {}
    j = 0
    
    for counter, i in enumerate(positions):
        
        fields[f'{str(counter)}'] = string[j:j+i]
        j += i
        
    return fields


#-------------------------------------------------------------------#
# key_concat
#-------------------------------------------------------------------#

def key_concat(fields, keys):
    
    '''
    Take a dict and a list of keys and return the concatenation of the fields \n
    in the list.
    fields: dict
    keys: list with the keys to concatenate
    '''
     
    keyConcat = ''
    for key in keys:
        keyConcat += fields[f'{key}']  
        
    return keyConcat


#-------------------------------------------------------------------#
# out_concat
#-------------------------------------------------------------------#      
        
def out_concat(fields, keys, record, uuid):
    
    '''
    Take two dicts, a list of keys and an uuid and returns the concatenation of \n
    the output.
    fields: dict (header)
    keys: list with the header's keys
    record: dict (records)
    uuid: header's uuid
    '''
    
    outConcat = ''
    for key in keys:
        outConcat += fields[f'{key}'] + '|'
        
    outConcat +=  record['2'] + '|' 'REC001' + '|'
    
    for key in record.keys():
        outConcat += record[f'{key}'] + '|'
        
    outConcat += 'RIFINPUT' + '|' + uuid
          
    return outConcat


#-------------------------------------------------------------------#  
# check_type
#-------------------------------------------------------------------#

def check_type(fields, types):
    
    '''
    Take a dict and a list with types to check whether the values of the dict \n
    correspond to the right type.
    fields: dict (header, record)
    types: list (values: 'A': Alphanumeric, 'N': numeric)
    '''
    
    for i, key in fields.items():
        
        if types[int(i)] == 'N':
            
            if key.isnumeric(): 
                return True  
            else:
                return False
     
            
#-------------------------------------------------------------------#
# check_data_format
#-------------------------------------------------------------------#     

def check_data_format(fields, types_data):
    
    '''
    Take a dict and a list with types to check whether a value of the dict,  \n
    having a data type YYYYMMDD, has a valid format, otherwise, the value is \n
    set to 0.
    fields: dict
    types_data: list with the keys of the dict that are data type.
    '''

    for key_data in types_data:
        
        data = fields[f'{key_data}']
        
        try: 
            datetime.datetime(int(data[0:4]), int(data[4:6]), int(data[6:]))
            valid_format = True
        
        except:
            valid_format = False
            fields[f'{key_data}'] = '00000000'
            
    return fields, valid_format
            

#-------------------------------------------------------------------#  
# msgcom
#-------------------------------------------------------------------#  
        
def msgcom(fields, record, uuid_cod):
    
    '''
    Take two dicts as input and a uuid code. Return the string concatenation to be \n
    written in the output file. \n
    fields: dict corresponding to the header. \n
    record: dict corresponding to the records. \n
    uuid_cod: uuid cod corresponding to code of the file.
    '''
    
    ts = datetime.datetime.now().strftime('%d-%m-%y %H:%M:%S.%f' )[:-3]
    uuid1 = str(uuid.uuid4())
    
    try:
        data = fields['4']
        ts_0 = datetime.datetime(int(data[0:4]), int(data[4:6]), int(data[6:]))
        ts_0 = str(ts_0.strftime('%d-%m-%y %H:%M:%S.%f' )[:-3])
        
    except:
        ts_0 = '0001-01-01 00:00:00.000'
        
    if fields['3'] == '00000000':
        ts_1 = '0001-01-01'
    else:
        ts_1 = fields['3']
    
    msgcom = uuid1 + '|' + fields['5'] + '|' + 'M' + '|' + record['2']         \
             + '|' + ts_0 + '|' + fields['2'] + '|' + ts_1 + '|'               \
             + '|' + '0' + '|' + record['8'] + '|' + '0' + '|' + ' ' + '|'     \
             + '0' + '|' + 'KO' + '|' + uuid_cod + '|' + 'Msg151ToCsv' + '|'   \
             +  ts + '|' + 'Msg151ToCsv' + '|' + ts   
    
    return msgcom



    
               
        
             
        
        
