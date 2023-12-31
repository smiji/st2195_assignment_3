import pandas as pd


def create_table_from_csv(table_name,csv_name,conn):
    print("Creating table,", table_name," from the csv file ,",csv_name)
    data_frame_from_csv = pd.read_csv(csv_name)
    print(data_frame_from_csv.head())   
    print('Writing to the database')    
    data_frame_from_csv.to_sql(table_name,con=conn,index=False,if_exists='append')
    print("Table ",table_name,"created successfully")    
    
