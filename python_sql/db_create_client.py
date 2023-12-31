import db_create as db_admin
import db_table_from_csv as db_client
import query_db as sql
from pathlib import Path


db_location='/Users/smiji.j/sqlite_dbs'
db_name='University.db'
student_csv='/Users/smiji.j/sqlite_dbs/db/student.csv'
grade_csv='/Users/smiji.j/sqlite_dbs/db/grade.csv'
course_csv='/Users/smiji.j/sqlite_dbs/db/course.csv'


def create_operate_student_db():
    db_admin.create_db(db_location,db_name)
    conn = db_admin.create_connection(db_location, db_name)
    db_client.create_table_from_csv("Student",student_csv,conn)
    db_client.create_table_from_csv("Course",course_csv,conn)
    db_client.create_table_from_csv("Grade",grade_csv,conn)
    db_admin.close_connection(conn)
    
    
    
db_location='/Users/smiji.j/sqlite_dbs'
db_name='airline.db'
#Create the ontime from 
#(with the data in 2006.csv, 2007.csv, and 2008)
base_path = '/Users/smiji.j/sqlite_dbs/db/'
ontime_csvs= ['2006.csv', '2007.csv', '2008.csv']
airport_csv='airports.csv'
carriers_csv='carriers.csv'
planes_csv = 'plane-data.csv'
conn = db_admin.create_connection(db_location, db_name)    
def create_operate_plane_db():
    db_admin.create_db(db_location,db_name)
    #Create ontime table
    for csv in ontime_csvs:
        db_client.create_table_from_csv("ontime",Path(base_path).joinpath(csv),conn)    
    db_client.create_table_from_csv("airports",Path(base_path).joinpath(airport_csv),conn)
    db_client.create_table_from_csv("carriers",Path(base_path).joinpath(carriers_csv),conn)
    db_client.create_table_from_csv("planes",Path(base_path).joinpath(planes_csv),conn)
    db_admin.close_connection(conn)
    
#create_operate_plane_db()
sql.get_plane_with_lowest_depart_delay(conn)
#sql.get_cities_with_highest_inbound_flights(conn)
#sql.get_companies_with_highest_cancelled_flights(conn)
#sql.get_companies_with_highest_cancelled_flights(conn)
#sql.get_companies_with_highest_cancel_density(conn)
#db_admin.close_connection(conn)
#df_planes = sql.get_all_planes(conn)
#print(df_planes.columns)
#df_planes.to_csv('plane_output.csv',index=False)
db_admin.close_connection(conn)

