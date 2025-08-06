import json
import logging
from src.utils import create_directory, directory_exists, concat_directory, split_directory
from src.utils import download_resource as r
from src.utils import read_json_file, get_all_files
from src.utils import abort_process

URL_API_MEETINGS="https://data.europarl.europa.eu/api/v2/meetings?year={0}&format=application%2Fld%2Bjson&offset=0&limit=1000"
URL_API_MEETING_DECISION = "https://data.europarl.europa.eu/api/v2/meetings/{0}/decisions?vote-method=ROLL_CALL_EV&format=application%2Fld%2Bjson&json-layout=framed-and-included&offset=0&limit=1000"

class api_meetings_ep:
    """"Call the Open Data Api -> 
        /EP Meetings
            /meetings(Year) = The service returns the list of all EP Meetings    
            /meetings/{sitting-id}/decisions = The service returns all decisions a single EP Meeting ID
    
    Input: config.yaml
    Returns: json files""
    """

    def __init__(self,config:dict):
        # Get meetings dictionary
        self.config = config["meetings"]
        self.user_agent = config["User-Agent"]
        self.folder_decision = config["folder_output_decision"]
        # Create Directory Output
        self.output = create_directory(config["dir_output_base"],None)
        # Log for meeting decision
        directory_exists(config['logging'])
        self.list_url_not_download = []
        self.log = logging.basicConfig(filename=f"{config['logging']}", level=logging.INFO)
        self.log = logging.getLogger(__name__)

    def __call_open_data_api(self,URL_MEETINGS:str, meeting_file:str) -> json:

        # Sending a GET request and getting back response.
        """ Call api URL and download the list of all MEP Meetings """
        response,response_status = r(URL_MEETINGS,self.user_agent)
        if response and response_status == 200:
            with open(meeting_file,"w") as fmeeting:
                json.dump(json.loads(response.data),fmeeting)
        elif response_status == 204:
            self.log.warning(f"Return status {response_status}")
        else:
            self.log.warning(f"Service currently unavailable - Cannot download: {URL_MEETINGS} or the service sended an error {response_status}")
            abort_process()
        
    def __download_meeting(self,MEETING_YEAR:str):

        """ Download all Meeting for year and save file """

        __FILE_NAME = f"meetings_{MEETING_YEAR}.json"
        __path_output = create_directory(self.output,str(MEETING_YEAR))
        __save_file = concat_directory(__path_output,__FILE_NAME)

        self.__call_open_data_api(URL_API_MEETINGS.format(MEETING_YEAR),__save_file)
    
    def __download_meeting_decision(self,path_meetings_file:str):

        """ Read a meeting file and download all meeting decision id """ 

        self.log.info(f"    Meeting: {path_meetings_file}")
        """ Read MEP Meetings"""
        json_content = read_json_file(path_meetings_file)
        directory_root,filename = split_directory(path_meetings_file)
        # Get all activity id in data
        all_activity_id = [d["activity_id"] for d in json_content["data"]]

        # Create decision directory for each year
        __path_output = create_directory(directory_root,self.folder_decision)        
        #
        self.log.info("    ** Download meeting decision **")
        self.list_url_not_download.clear()
        for activity_id in all_activity_id:
            __file_meeting_decision = f"{activity_id}.json"
            __output_file_meeting_decision = concat_directory(__path_output,__file_meeting_decision)
            #
            self.__call_open_data_api(URL_API_MEETING_DECISION.format(activity_id),__output_file_meeting_decision)
            
        """ Show the list of the MEP meetings cannot possible download. """
        if len(self.list_url_not_download) > 0:
            self.log.info(f"Warning: Downloads failed: {',\n'.join(self.list_url_not_download)}")
        
    def __get_meetings(self):

        print("The list of all EP Meetings.")
        self.log.info("The list of all EP Meetings.")

        [self.__download_meeting(meeting_year) for meeting_year in self.config["years"]]
        """ Show the list of the MEP meetings cannot possible download. """
        if len(self.list_url_not_download) > 0:
            self.log.info(f"Warning: Downloads failed: {',\n'.join(self.list_url_not_download)}")
        
        print("The meetings download is end. ")
        self.log.info("The meetings download is end. ")

    def __get_meeting_decision(self):

        print("All decisions a single EP Meeting ID.")
        self.log.info("All decisions a single EP Meeting ID.")
        
        [self.__download_meeting_decision(meeting_year_file) for meeting_year_file in get_all_files(self.output)]
        print("The decisions processes is end.")
        self.log.info("The decisions processes is end.")
    
    def get_meeting_resources(self):
        # for Meeting
        self.__get_meetings()
        # for Meeting Decision
        self.__get_meeting_decision()