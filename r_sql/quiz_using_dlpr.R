
load_all_df_from_db <- function(){
  library(DBI)
  path = "/Users/smiji.j/sqlite_dbs/airline.db"
  conn = dbConnect(RSQLite::SQLite(),path)
  sql="select * from ontime"
  ontime = dbGetQuery(conn,sql)
  sql = "select * from planes"
  planes = dbGetQuery(conn,sql)
  
  sql = "select * from carriers"
  carriers = dbGetQuery(conn,sql)
  
  sql = "select * from airports"
  airports = dbGetQuery(conn,sql)
  dbDisconnect(conn)
  nrow(ontime)
  nrow(airports)
  nrow(planes)
  nrow(carriers)
  
}

# Use onetime 
#load_all_df_from_db()

cities_with_highest_inbound_flights <- summarise(group_by(only_req_columns,city),max_city=max(n())) %>% arrange(desc(max_city)) %>% slice_head(n = 4)
print(cities_with_highest_inbound_flights)