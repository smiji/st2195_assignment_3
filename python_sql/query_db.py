import pandas as pd

def get_companies_with_highest_cancelled_flights(conn):
    cur = conn.cursor()
    cur.execute('''
                SELECT model AS model, AVG(ontime.DepDelay) 
                AS avg_delay
                FROM planes JOIN ontime USING(tailnum)
                WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 
                AND ontime.DepDelay > 0 GROUP BY model ORDER BY avg_delay
                ''')
    selected = cur.fetchone()
    print(type(selected))
    print(selected)
         
    
def get_companies_with_highest_cancel_density(conn):
    cur = conn.cursor()
    cur.execute('''
                  SELECT q1.carrier AS carrier, 
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
                     ''')
    selected = cur.fetchone() 
    print(type(selected))
    print(selected)
      
      
def get_companies_with_highest_cancelled_flights(conn):
    cur = conn.cursor()
    cur.execute(''' 
                SELECT carriers.Description AS carrier, COUNT(*) AS total
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
                ORDER BY total DESC
                ''')
    selected = cur.fetchone() 
    print(type(selected))
    print(selected)
     
def get_cities_with_highest_inbound_flights(conn):
    cur = conn.cursor()
    cur.execute(''' 
                SELECT airports.city AS city, COUNT(*) AS total
                FROM airports JOIN ontime ON ontime.dest = airports.iata
                WHERE ontime.Cancelled = 0
                GROUP BY airports.city
                ORDER BY total DESC
                ''')
    selected = cur.fetchone() 
    print(type(selected))
    print(selected)
     

def get_all_planes(conn):
    c=conn.cursor()
    c.execute('''
              select * from planes
              ''')          
    dataset_planes=pd.DataFrame(c.fetchall())
    return dataset_planes

    
    
    
    