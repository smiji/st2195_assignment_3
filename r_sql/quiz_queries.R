
get_plane_with_lowest_depart_delay <- function(path_to_db,path_to_output){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  sql <- "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
          FROM planes JOIN ontime USING(tailnum)
          WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
          GROUP BY model
          ORDER BY avg_delay"
  result <- dbGetQuery(conn,sql)
  result_head <- head(result,3)
  write.csv(result_head,path_to_output, row.names=FALSE)
  dbDisconnect(conn)
  print(paste("Query completed , please find the output at ..",path_to_output))
}


get_companies_with_highest_cancel_density <- function(path_to_db,path_to_output){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  sql<- "SELECT q1.carrier AS carrier, 
                  (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
                  FROM (
                      SELECT carriers.Description AS carrier, COUNT(*) 
                      AS numerator
                      FROM carriers JOIN ontime 
                      ON ontime.UniqueCarrier = carriers.Code
                      WHERE ontime.Cancelled = 1 AND 
                      carriers.Description IN 
                      ('United Air Lines Inc.', 
                       'American Airlines Inc.', 
                       'Pinnacle Airlines Inc.', 
                       'Delta Air Lines Inc.')
                      GROUP BY carriers.Description) AS q1 JOIN
                      (
                      SELECT carriers.Description AS carrier, 
                      COUNT(*) AS denominator
                      FROM carriers JOIN ontime 
                      ON ontime.UniqueCarrier = carriers.Code
                      WHERE carriers.Description IN 
                      ('United Air Lines Inc.', 
                       'American Airlines Inc.', 
                       'Pinnacle Airlines Inc.', 
                       'Delta Air Lines Inc.')
                      GROUP BY carriers.Description
                      ) AS q2 USING(carrier)
                      ORDER BY ratio DESC  
                     "
  result <- dbGetQuery(conn,sql)
  result_head <- head(result,3)
  write.csv(result_head,path_to_output, row.names=FALSE)
  dbDisconnect(conn)
  print(paste("Query completed , please find the output at ..",path_to_output))
  }

get_companies_with_highest_cancelled_flights <- function(path_to_db,path_to_output){
    library(DBI)
    conn <- dbConnect(RSQLite::SQLite(),path_to_db)
  sql<- " SELECT carriers.Description AS carrier, COUNT(*) AS total
                FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                WHERE ontime.Cancelled = 1
                AND carriers.Description IN 
                (
                    'United Air Lines Inc.', 
                    'American Airlines Inc.', 
                    'Pinnacle Airlines Inc.', 
                    'Delta Air Lines Inc.'
                )
                GROUP BY carriers.Description
                ORDER BY total DESC"
  result <- dbGetQuery(conn,sql)
  result_head <- head(result,3)
  write.csv(result_head,path_to_output, row.names=FALSE)
  dbDisconnect(conn)
  print(paste("Query completed , please find the output at ..",path_to_output))
}

get_cities_with_highest_inbound_flights <- function(path_to_db,path_to_output){
  library(DBI)
  conn = dbConnect(RSQLite::SQLite(),path_to_db)
  sql<- "SELECT airports.city AS city, COUNT(*) AS total
                FROM airports JOIN ontime ON ontime.dest = airports.iata
                WHERE ontime.Cancelled = 0
                GROUP BY airports.city
                ORDER BY total DESC"
  
  result <- dbGetQuery(conn,sql)
  result_head <- head(result,3)
  write.csv(result_head,path_to_output, row.names=FALSE)
  dbDisconnect(conn)
  print(paste("Query completed , please find the output at ..",path_to_output))  
}







path = "/Users/smiji.j/sqlite_dbs/airline.db"
path_to_output = "/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql/r_output/plane_with_lowest_depart_delay.csv"
get_plane_with_lowest_depart_delay(path,path_to_output)



path = "/Users/smiji.j/sqlite_dbs/airline.db"
path_to_output = "/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql/r_output/companies_with_highest_cancel_density.csv"
get_companies_with_highest_cancel_density(path,path_to_output)



path = "/Users/smiji.j/sqlite_dbs/airline.db"
path_to_output = "/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql/r_output/companies_with_highest_cancelled_flights.csv"
get_companies_with_highest_cancelled_flights(path,path_to_output)


path = "/Users/smiji.j/sqlite_dbs/airline.db"
path_to_output = "/Users/smiji.j/DataScienceProgramming/st2195_assignment_3/r_sql/r_output/cities_with_highest_inbound_flights.csv"
get_cities_with_highest_inbound_flights(path,path_to_output)
