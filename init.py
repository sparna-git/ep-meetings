from src.utils import read_configuration_file
from src.meetings import api_meetings_ep
from src.transform_votes import meps_decision

if __name__ == "__main__":

    # Read config.yaml file
    config = read_configuration_file("config.yaml")

    """ Get meetings resources for each year configurated in the config.yaml file and all decisions a single EP Meeting ID. """
    meetings_year = api_meetings_ep(config)
    meetings_year.get_meeting_resources()

    """ Read all decision files and generate a dataset """
    statistic_votes = meps_decision(config)
    statistic_votes.dataset_decision()

    print("End process....")