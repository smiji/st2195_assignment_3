# =============================================================================
# Python soultion to question-4
# Which city has the highest inboud flights
# Use airport.csv for cities and ontime.csv for
# Flights count and cancelled information 
#=============================================================================
# Read the csv from the dataset path
from pathlib import Path
import pandas as pd


def read_csv_to_df(csv_name):
    current_working_directory = Path.cwd()
    csv_path = current_working_directory.parent.joinpath('Dataset').joinpath(csv_name)
    try :
        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        print('File reading problems ,invalid file name',csv_name)

def append_data_frames(df1,df2):        
    return pd.concat([df1,df2],axis=0)

def prepare_ontime():
    #Combaining the 2006,2007,2008 dataset to for the ontime.csv
    df_ontime_2006 = read_csv_to_df('2006.csv')
    df_ontime_2007 = read_csv_to_df('2007.csv')   
    df_ontime_2008 = read_csv_to_df('2008.csv')
    df_ontime = pd.DataFrame(append_data_frames(df_ontime_2008,pd.DataFrame(append_data_frames(df_ontime_2006,df_ontime_2007))))
    return df_ontime

def get_highest_inbound_city(all_data):
    print()
    # Exclude the cancelled flights
    exclude_condition = all_data['Cancelled']!=0
    all_data = pd.DataFrame(all_data[exclude_condition])
    print(all_data
            .groupby(["city"]).size() 
            .sort_values(ascending=False) 
            .reset_index(name='count') 
            .drop_duplicates(subset='city').head(1))      

    
df_airport= read_csv_to_df('airports.csv')
df_ontime = prepare_ontime() 
all_data = pd.merge(df_ontime,df_airport,left_on='Dest', right_on='iata',how="inner")
get_highest_inbound_city(all_data)






















