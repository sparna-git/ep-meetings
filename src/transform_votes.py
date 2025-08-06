import pandas as pd
from src.utils import get_directory_absolute, split_directory, read_json_file, evalue_condition, get_all_decisions, directory_exists
import logging

# List of the columns selected for work
# for votes 
list_columns_votes=[
                    "had_voter_abstention",
                    "had_voter_against",
                    "had_voter_favor"                   
                    ]
# for meps
list_columns_meps = [
                    "id",
                    "identifier",
                    "label",
                    "api:political-group"
                    ]

class meps_decision:

    """
    This process read and transform the meeting decision json files to a csv file.
    """

    def __init__(self,config:dict):

        self.resources = get_directory_absolute(config["dir_output_base"])
        self.filter_decision = config["filter_decision"]
        self.dataset_meps = pd.DataFrame()
        self.output_file = config["dir_output_file"]
        # Log for meeting decision
        self.log = logging.getLogger(__name__)
    
    def __populate_dataframe(self,inputData:list) -> list:
        """ Convert dict to dataframe """
        records = pd.DataFrame(inputData)
        return records
    
    def __get_mep_voter(self,dfPivot:pd.DataFrame) -> pd.DataFrame:
        
        """ if the cell value is a list type else transpose data from columns to rows """

        # Instantiate an empty DataFrame
        dfOutput = pd.DataFrame()
        if isinstance(dfPivot.iloc[0][dfPivot.columns[0]],list):
            df = dfPivot.explode(dfPivot.columns[0], ignore_index=True)            
            dfOutput = df.rename(columns={f"{dfPivot.columns[0]}":"had_voter_mep"})
            dfOutput["vote_type"] = [dfPivot.columns[0] for idx in range(0,df.index.max()+1)]            
        return dfOutput

    def __convert_votes_in_table(self,df:pd.DataFrame) -> pd.DataFrame:

        """ for each vote type get the column . """
        list_of_result = []
        for voteId in list_columns_votes:
            if voteId in df.columns.to_list():
                df_new_structure = self.__get_mep_voter(df[[voteId]])
                list_of_result.append(df_new_structure)    
        dfTable = pd.concat(list_of_result,ignore_index=True,axis=0)
        return dfTable

    def __votes_dataset(self,dataset_votes:list) -> pd.DataFrame:

        dataset_result_votes = []
        for __activityId in dataset_votes.activity_id.to_list():
            # Filter for id
            dfActivityId = dataset_votes[dataset_votes["activity_id"] == __activityId]
            # Replaces les cell Nan for empty
            dfOperation = dfActivityId.fillna("")
            
            """ Section of traitement data """
            # Get Englis label
            dict_label = dfOperation["activity_label"].values[0]
            dfOperation["activity_label"] = dict_label["en"]
            # Get the result of convert all list column to rows
            dfVotes = self.__convert_votes_in_table(dfOperation)
            # Get the number of row in dataframe report
            nIndex = dfVotes.index.max()
            # Add "activity_id","activity_date","activity_label" data            
            dfVotes["activity_id"] = [dfOperation["activity_id"].values[0] for idx in range(0,nIndex+1)]
            dfVotes["activity_date"] = [dfOperation["activity_date"].values[0] for idx in range(0,nIndex+1)]
            dfVotes["activity_label"] = [dfOperation["activity_label"].values[0] for idx in range(0,nIndex+1)]

            # Remove person/{Id Meps}
            dfVotes.replace(regex=r'person/',value="",inplace=True)
            # Get the Meps data and joint in the result.
            dfResult = dfVotes.merge(self.dataset_meps,left_on="had_voter_mep",right_on="had_voter")
            # Save in list
            dataset_result_votes.append(dfResult[["activity_id","activity_date","had_voter","label","api:political-group","vote_type","activity_label"]])

        # Return dataframe result
        return pd.concat(dataset_result_votes)

    def __rename_type_of_vote(self, vote_type:str) -> str:

        eq_vote_type = {
            "had_voter_favor":"FOR",
            "had_voter_abstention":"ABSTENTION",
            "had_voter_against":"AGAINST"
            }
        
        return eq_vote_type[vote_type]

    def __load_in_dataframe(self,inputFile:str):

        print(f"Read {inputFile} meeting decision file.")
        self.log.info(f"Read {inputFile} meeting decision file.")

        directory_root,filename = split_directory(inputFile)

        """ Read json file and return a dictionary"""
        meeting = read_json_file(inputFile)

        dir_root,filename = split_directory(inputFile)
        if evalue_condition(self.filter_decision,filename):
            print(filename)

            """ Get Votes and Meps Section """
            __df_votes = self.__populate_dataframe(meeting["data"])
            __df_meps = self.__populate_dataframe(meeting["included"])
            
            # Filter the columns for to meps
            __dfMeps = __df_meps[list_columns_meps]
            # Rename identifier column to had_voter
            self.dataset_meps = __dfMeps.rename(columns={"identifier":"had_voter"})
            
            # Build a dataset of votes
            votes_dataset = self.__votes_dataset(__df_votes)
            votes_dataset["vote_type"] = votes_dataset["vote_type"].apply(self.__rename_type_of_vote)
            
            return votes_dataset
    
    def dataset_decision(self):
        print("Statistic - Read meeting decision")
        self.log.info("Statistic - Read meeting decision")

        dataset_votes = []
        [dataset_votes.append(self.__load_in_dataframe(m)) for m in get_all_decisions(self.resources)]
        
        # Output dataframe result
        dfOutput = pd.concat(dataset_votes)
        # Sort for activity id
        dfOutput.sort_values("activity_id")

        self.log.info(f"Number of line: {len(dfOutput)}")
        # Save in CSV file
        directory_exists(self.output_file)
        dfOutput.to_csv(self.output_file,index=False)
        print(f"The {self.output_file} file was created in the directory...")
        self.log.info(f"The {self.output_file} file was created in the directory...")