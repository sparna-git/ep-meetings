import json
import os
import glob
import shutil
from pathlib import Path
from yaml import safe_load # Yaml
import re # regex
import urllib3 # requests download
import time
import logging
log = logging.getLogger(__name__)


def read_configuration_file(configYaml:str):

    """ 
    Read the configuration file: config.yaml\n
    @Return = dict type
    """

    path: Path = Path(configYaml)
    # Read file and parse with yaml
    return safe_load(path.read_text())

def read_json_file(inputFile:str) -> dict:
    """ Read a json file """
    with open(inputFile,"r") as rJson:
            return json.load(rJson) 


def create_directory(path_dir:str,path_folder:str):

    print(f"Directory: {path_dir} subdirectory: {path_folder}")

    path_output = ""
    if path_dir and path_folder:
        path_output = os.path.join(path_dir,path_folder)   
    elif path_dir and not path_folder:
         path_output = Path(path_dir).absolute()
          
    if not os.path.exists(path_output):
        log.info(f"Create directory: {path_output}")
        os.mkdir(path_output)
    else:
        shutil.rmtree(path_output)
        os.mkdir(path_output)
    return path_output

def split_directory(pat_directory:str):
    return os.path.split(pat_directory)

def get_directory_absolute(directory_resource:str):
    return Path(directory_resource).absolute()

def concat_directory(path_dir_base:str,path_dir_output:str):
    return os.path.join(path_dir_base,path_dir_output)

def directory_exists(path_directory:str):
    
    """ Create directory if not exist else delete directory recreate .  """

    dir_root, filename = split_directory(path_directory)
    if not os.path.exists(dir_root):
        os.mkdir(dir_root)
    else:
        shutil.rmtree(dir_root)
        os.mkdir(dir_root)

def get_all_files(path_resource:str) -> list:
    """
     Return the list of meeting files downloaded for year. 
    """
    list_output = glob.glob("**/*.json",root_dir=path_resource,recursive=True)
    return [os.path.join(path_resource,f) for f in list_output]

def get_all_decisions(path_resource:str) -> list:
    """
     Return the list of meeting decision files downloaded. 
    """
    list_output = glob.glob("**/*/decision/*.json",root_dir=path_resource,recursive=True)
    return [os.path.join(path_resource,f) for f in list_output]


def download_resource(URL_MEETING_YEAR:str,USER_AGENT:str) -> dict:
    """ Download meetings resource """
    time.sleep(3)
    

    # Create an HTTPHeaderDict and add headers
    headers = {'user-agent': f'{USER_AGENT}'}

    # Sending a GET request and getting back response.
    resp = urllib3.request("GET",URL_MEETING_YEAR,headers=headers,timeout=urllib3.Timeout(connect=1.0)) 
    
    if resp.status == 200:
        log.info(f"URL: {URL_MEETING_YEAR}")         
    elif resp.status == 204 :
        log.warning(f"URL: {URL_MEETING_YEAR}")
    elif resp.status == 500 :
        log.warning(f"URL: {URL_MEETING_YEAR}")
    else:
        log.warning(f"URL: {URL_MEETING_YEAR}")
    return resp, resp.status
     
def evalue_condition(pattern:str,filename:str) -> bool:
    """ Filter the date with a pattern configurated in the confiog.yaml file """
    return re.match(pattern,filename)

def abort_process():
    os.abort()