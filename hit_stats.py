#!/usr/bin/env python3                                                                                                                                                       
                                                                                                                                                                             
import os                                                                                                                                                                    
import re                                                                                                                                                                    
import sys                                                                                                                                                                   
import argparse                                                                                                                                                              
import fnmatch                                                                                                                                                               
import gzip                                                                                                                                                                  
import bz2                                                                                                                                                                   
from pprint import pprint                                                                                                                                                    
                                                                                                                                                                             
                                                                                                                                                                             
def parse_cli():                                                                                                                                                             
    parser = argparse.ArgumentParser()                                                                                                                                       
    parser.add_argument(                                                                                                                                                     
        "-f",                                                                                                                                                                
        "--files",                                                                                                                                                           
        help="log files(text, .zip, .bz2, gzip) to be extracted seperated by comma(file1.log,file2.log,filen.log)",                                                          
    )                                                                                                                                                                        
    parser.add_argument(                                                                                                                                                     
        "-u",                                                                                                                                                                
        "--url-hits",                                                                                                                                                        
        default=False,                                                                                                                                                       
        action="store_true",                                                                                                                                                 
        help="stats on unique url hits",                                                                                                                                     
    )                                                                                                                                                                        
    parser.add_argument(                                                                                                                                                     
        "-a",                                                                                                                                                                
        "--api-hits",                                                                                                                                                        
        default=False,                                                                                                                                                       
        action="store_true",                                                                                                                                                 
        help="stats on unique api hits",                                                                                                                                     
    )                                                                                                                                                                        
    args = parser.parse_args()                                                                                                                                               
    return args                                                                                                                                                              
                                                                                                                                                                             
                                                                                                                                                                             
def fetch_files(files):                                                                                                                                                      
    for file_name in files.split(","):                                                                                                                                       
        yield file_name.strip()                                                                                                                                              
                                                                                                                                                                             
                                                                                                                                                                             
def file_opener(filenames):                                                                                                                                                  
    for file_name in filenames:                                                                                                                                              
        if file_name.endswith(".gz"):                                                                                                                                        
            fh = gzip.open(file_name, "rt")                                                                                                                                  
        elif file_name.endswith(".bz2"):                                                                                                                                     
            fh = bz2.open(filename, "rt")                                                                                                                                    
        else:                                                                                                                                                                
            fh = open(file_name, "rt")                                                                                                                                       
        yield fh                                                                                                                                                             
        fh.close()                                                                                                                                                           
                                                                                                                                                                             
                                                                                                                                                                             
def concatenator(iterators):                                                                                                                                                 
    for it in iterators:                                                                                                                                                     
        yield from it                                                                                                                                                        
                                                                                                                                                                             
                                                                                                                                                                             
def extract(lines):                                                                                                                                                          
    re_pattern = r"(?P<ip>.*?)(?P<remote_log_name>.*?) (?P<userid>.*?) (?P<dummy1>.*?) (?P<dummy2>.*?) \[(?P<date>.*?)(?= ) (?P<timezone>.*?)\] (?P<dummy3>.*?) \"(?P<req_met
hod>.*?) (?P<api>.*?) HTTP/(\d\.\d)\" (?P<extra_path>.*?)(?P<status_code>\d+?) (.*?) \"(?P<url>.*?)\" (.*)"                                                                  
    for line in lines:                                                                                                                                                       
        matched = re.search(re_pattern, line.strip())                                                                                                                        
        if matched:                                                                                                                                                          
            _date = matched.group("date").split(":")[0]                                                                                                                      
            _api = "/".join(matched.group("api").split("/")[:4])                                                                                                             
            _url = matched.group("url")                                                                                                                                      
            yield (                                                                                                                                                          
                _date,                                                                                                                                                       
                _api,                                                                                                                                                        
                _url,                                                                                                                                                        
            )                                                                                                                                                                
                                                                                                                                                                             
                                                                                                                                                                             
def main():                                                                                                                                                                  
    re_pattern = r"(?P<ip>.*?)(?P<remote_log_name>.*?) (?P<userid>.*?) (?P<dummy1>.*?) (?P<dummy2>.*?) \[(?P<date>.*?)(?= ) (?P<timezone>.*?)\] (?P<dummy3>.*?) \"(?P<req_met
hod>.*?) (?P<api>.*?) HTTP/(\d\.\d)\" (?P<extra_path>.*?)(?P<status_code>\d+?) (.*?) \"(?P<url>.*?)\" (.*)"                                                                  
                                                                                                                                                                             
    cli = parse_cli()                                                                                                                                                        
    lognames = fetch_files(cli.files)                                                                                                                                        
    files = file_opener(lognames)                                                                                                                                            
    lines = concatenator(files)                                                                                                                                              
    extracted = extract(lines)                                                                                                                                               
                                                                                                                                                                             
    payload = {}                                                                                                                                                             
    if cli.url_hits:                                                                                                                                                         
        for date, _, url in extracted:                                                                                                                                       
            if not date in payload.keys():                                                                                                                                   
                payload[date] = {}                                                                                                                                           
            if not url in payload[date].keys():                                                                                                                              
                payload[date][url] = 1                                                                                                                                       
            else:                                                                                                                                                            
                payload[date][url] += 1                                                                                                                                      
        for k in payload.keys():                                                                                                                                             
            print(f"{'-' * 30}")                                                                                                                                             
            print("{:^30s}".format(k))                                                                                                                                       
            print(f"{'-' * 30}")                                                                                                                                             
            for key, value in sorted(                                                                                                                                        
                payload[k].items(), reverse=True, key=lambda item: item[1]                                                                                                   
            ):                                                                                                                                                               
                print("{:<25s}: {}".format(key, value))                                                                                                                      
                                                                                                                                                                             
    if cli.api_hits:                                                                                                                                                         
        for date, api, _ in extracted:                                                                                                                                       
            if not date in payload.keys():                                                                                                                                   
                payload[date] = {}                                                                                                                                           
            if not api in payload[date].keys():                                                                                                                              
                payload[date][api] = 1                                                                                                                                       
            else:                                                                                                                                                            
                payload[date][api] += 1                                                                                                                                      
        for k in payload.keys():                                                                                                                                             
            print(f"{'-' * 65}")                                                                                                                                             
            print("{:^65s}".format(k))                                                                                                                                       
            print(f"{'-' * 65}")                                                                                                                                             
            for key, value in sorted(                                                                                                                                        
                payload[k].items(), reverse=True, key=lambda item: item[1]                                                                                                   
            ):                                                                                                                                                               
                print("{:<60s}: {}".format(key[:50], value))                                                                                                                 
                                                                                                                                                                             
                                                                                                                                                                             
if __name__ == "__main__":                                                                                                                                                   
    main()                                                                                                                                                                   
