# -*- coding: utf-8 -*-
"""
Created on Mon May 16 09:55:29 2022

@author: luigi
"""


from header import *
import os



start = datetime.datetime.now()
timestamp = start.strftime('%d_%m_%y_%H_%M' )
# base path
path =os.path.dirname(__file__)


#-----------------------------------------------------------------------------------------------#
# write start to log
#-----------------------------------------------------------------------------------------------#
with open(f'{path}/log/log_{timestamp}.log', 'a') as out:
    out.write(f'Process started:{datetime.datetime.now()} \n')


#-----------------------------------------------------------------------------------------------#
# get metadata
#-----------------------------------------------------------------------------------------------#
metadata = open(f'{path}/metadata.txt')
metadata = [[int(j) for j in i.split(sep=',')] 
            if i.rstrip().replace(',', '').isnumeric() 
            else list(i.rstrip().replace(',', '')) for i in metadata.readlines()]

#-----------------------------------------------------------------------------------------------#
#setting constants
#-----------------------------------------------------------------------------------------------# 
head_field_len = metadata[0]    #len of each field in the header
rec_field_len = metadata[1]     #len of each field in the record
key_head = metadata[2]          #fields from the hearder to use in the key 
key_rec01 = metadata[3]         #fields from the rec to use in the key           
head_data = metadata[4]         #fields from the header which are dates    
rec_data = metadata[5]          #fields from the record which are dates              
head_types =metadata[6]         #types of data, header: A: Alphanumeric; N: numeric
record_types = metadata[7]      #types of data, rec0001
output_header = metadata[8]     #order output header
foot_field_len = metadata[9]    #len of each field in the footer
foot_types = metadata[10]       #types of data, footer: A: Alphanumeric; N: numeric


#-----------------------------------------------------------------------------------------------#
# get input file
#-----------------------------------------------------------------------------------------------#
dataset = open(f'{path}/input/19271_FT_CRR_151_20220420172615780_input.DAT', 'r')
lines = dataset.readlines()
num_rec = len(lines)


#-----------------------------------------------------------------------------------------------#
# process dataset
#-----------------------------------------------------------------------------------------------#
output_final = ''
for i, line in enumerate(lines):
    
    #print(i, line)
    # header
    if line.split(sep='|')[1].startswith('ANABI'):     
    
        head = line
        head_0, head_1 = header47(head)
        headers = split_fields(head_1, head_field_len)
        
        
        #checks
        header_check_types = check_type(headers, head_types)
        headers, header_check_format = check_data_format(headers, head_data)
        
        #concatenate keys
        header_keys = key_concat(headers, key_head)
        
        rec_keys = ''
        key_new = ''
    
    # records
    elif not line.split(sep='|')[1].startswith('CODBI'):
        
        record = line
        rec_0, rec1 = recSplit(record)
        rec = split_fields(rec1, rec_field_len)
        
        #checks
        rec, record_check_format = check_data_format(rec, rec_data)
        record_check_types = check_type(rec, record_types)
        
        #concatenate keys records
        rec_keys = key_concat(rec, key_rec01)
        
        # output
        output = out_concat(headers, output_header, rec, head_0) + '\n'

        
        key_new =  header_keys + rec_keys
        duplicates_keys = key_new == key_old
        
        
        # if there are no errors in the header types
        if header_check_types:
            
            # if the record is not a duplicate and the types of the fields are right
            if not duplicates_keys and record_check_types:
                
                # add the record to the csv of valid ones
                output_final += output
                
                    
            # if the record is a duplicate or the fields are not right       
            else:
                
                output = msgcom(headers, rec, head_0)
                
                # add the record to the csv of invalid ones
                with open(f'{path}/output/SPLITTER_FW_MSGCOM_{timestamp}.csv', 'a') as out:
                    out.write(output + '\n')
                
                # add the ckecks to the log
                with open(f'{path}/log/log_{timestamp}.log', 'a') as out:
                    out.write(f'In line {i}:    DuplicateKeys:{duplicates_keys},' + 
                              f'HeaderCheckTypes:{"OK" if header_check_types else "KO"},' +  
                              f'RecordCheckTypes:{"OK" if record_check_types else "KO"},' +     
                              f'HeaderCheckFormat:{"OK" if header_check_format else "KO"},' +    
                              f'RecordCheckFormat:{"OK" if record_check_format else "KO"} \n')   
                
                    
        else:
            output = msgcom(headers, rec, head_0)
            with open(f'{path}/output/SPLITTER_FW_MSGCOM_{timestamp}.csv', 'a') as out:
                out.write(output + '\n')
            
            with open(f'{path}/log/log_{timestamp}.log', 'a') as out:
                out.write(f'In line {i}:    DuplicateKeys:{duplicates_keys},' + 
                          f'HeaderCheckTypes:{"OK" if header_check_types else "KO"},' +  
                          f'RecordCheckTypes:{"OK" if record_check_types else "KO"},' +     
                          f'HeaderCheckFormat:{"OK" if header_check_format else "KO"},' +    
                          f'RecordCheckFormat:{"OK" if record_check_format else "KO"} \n')    

    # footer
    else:
        foot = line
        foot_0, foot_1 = footer(foot)
        footers = split_fields(foot_1, foot_field_len)
        
        #checks
        footer_check_types = check_type(footers, foot_types)

        # check footer types
        if footer_check_types:
        
            with open(f'{path}/output/output_{timestamp}.DAT', 'a') as out:
                out.write(output_final)
        
        else:
            with open(f'{path}/log/log_{timestamp}.log', 'a') as out:
                out.write(f'In line {i}:    DuplicateKeys:{duplicates_keys},' + 
                          f'HeaderCheckTypes:{"OK" if header_check_types else "KO"},' +  
                          f'RecordCheckTypes:{"OK" if record_check_types else "KO"},' +     
                          f'HeaderCheckFormat:{"OK" if header_check_format else "KO"},' +    
                          f'RecordCheckFormat:{"OK" if record_check_format else "KO"}' +
                          f'FooterCheckType{footer_check_type}    \n')  
            
        
    # outside if, elif, else
    # update old key
    key_old = key_new


#-----------------------------------------------------------------------------------------------#
# end process dataset
#-----------------------------------------------------------------------------------------------#


#-----------------------------------------------------------------------------------------------#     
# write end to log
#-----------------------------------------------------------------------------------------------#
with open(f'{path}/log/log_{timestamp}.log', 'a') as out:
    out.write(f'Process ended:{datetime.datetime.now()} \n' + 
              f'Duation: {datetime.datetime.now() - start}')        
    
    
    

    







