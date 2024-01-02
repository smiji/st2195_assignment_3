#Create database 
#Create tables from csvs
#install.packages("RSQLite")
remove_db_if_exists <- function (path){
  print(path)
  if (file.exists(path)){
    print("Removing the existing database..")
    file.remove(path)
  }  
}

merge_data_frame <- function (){
  path_csv = "/Users/smiji.j/sqlite_dbs/db"
  setwd(path_csv)
  df_for_csv1 <- read.csv("2006.csv",header = TRUE)
  df_for_csv2 <- read.csv("2007.csv",header = TRUE)
  df_for_csv3 <- read.csv("2008.csv",header = TRUE)
  merged_df <- rbind(rbind(df_for_csv1,df_for_csv2),df_for_csv3)
  return (merged_df) 
}

prepare_tables_from_csv<-function(conn,working_dir,csv_name,table_name){
  setwd(working_dir)
  df_for_csv <- read.csv(csv_name,header = TRUE)
  dbWriteTable(conn,table_name,df_for_csv)
}


create_tables_from_csv <- function(path_to_db){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  path_csv = "/Users/smiji.j/sqlite_dbs/db"
  prepare_tables_from_csv(conn,path_csv,"airports.csv","airports")
  prepare_tables_from_csv(conn,path_csv,"carriers.csv","carriers")
  prepare_tables_from_csv(conn,path_csv,"plane-data.csv","planes")
  dbWriteTable(conn,'ontime',merge_data_frame())
  dbDisconnect(conn)
}


summary_of_tables_created <- function(path_to_db){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  print(dbReadTable(conn,"planes"))
  dbDisconnect(conn)
}

path = "/Users/smiji.j/sqlite_dbs/airline.db"
remove_db_if_exists(path)
create_tables_from_csv(path)
summary_of_tables_created(path)


#setwd("/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql")
#source("db_queries.R")
#getGradeFromCourseId(conn)

