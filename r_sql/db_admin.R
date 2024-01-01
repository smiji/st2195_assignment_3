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

create_tables_from_csv <- function(path_to_db){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  path_csv = "/Users/smiji.j/sqlite_dbs/db"
  prepare_tables_from_csv(conn,path_csv,"student.csv","Student")
  prepare_tables_from_csv(conn,path_csv,"course.csv","Course")
  prepare_tables_from_csv(conn,path_csv,"grade.csv","Grade")
  dbDisconnect(conn)
}

prepare_tables_from_csv<-function(conn,working_dir,csv_name,table_name){
  setwd(working_dir)
  df_for_csv <- read.csv(csv_name,header = TRUE)
  dbWriteTable(conn,table_name,df_for_csv)
}

summary_of_tables_created <- function(path_to_db){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  print(dbReadTable(conn,"Grade"))
  dbDisconnect(conn)
}

path = "/Users/smiji.j/sqlite_dbs/University.db"
remove_db_if_exists(path)
create_tables_from_csv(path)
summary_of_tables_created(path)



#setwd("/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql")
#source("db_queries.R")
#getGradeFromCourseId(conn)

