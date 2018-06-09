import csv
from cassandra.cluster import Cluster
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

#Creates Keyspace and table and inserts the data from CSV into the table
class dbconnect:
    def __init__(self):


	session.execute(
  	  """
  	  CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {
      	  'class' : 'SimpleStrategy',
      	  'replication_factor' : 1
  	  } 	
  	  """
	)

	session.set_keyspace('demo')


	session.execute(
   	"""
   	CREATE TABLE CASSANDRA_METRICS (
       	timestamp text PRIMARY KEY,
       	livesstablecount int,
       	memtablesize int,
       	latency float
       
   	);
   	"""
	)

    def insert_db(self):
        flag_lsstable = 0
        flag_memtsize = 0
        flag_latency = 0
        try:
             with open("jmxmetrics.csv") as fh:
                 lines = fh.readlines()

             	 for i in range(0,len(lines)):
                     print(lines[i])
                     val1=[]
                     val1=lines[i].strip().split(",")
                     print(val1[0])
                     print(val1[1])
              
                     flag_lsstable = flag_lsstable or val1[1]
                     flag_memtsize = flag_memtsize or val1[2]
                     flag_latency = flag_latency or val1[3]


       
                     session.execute(
                     """
                  	INSERT INTO CASSANDRA_METRICS (timestamp, livesstablecount, memtablesize, latency)
                  	VALUES (%s,%s,%s,%s)
                     """,
                  	(str(val1[0]),int(val1[1]),int(val1[2]),float(val1[3]))
                     )
                   
                     #Assertion check to see if the recorded metrics are non-negative

                     try:
                          assert int(val1[1]) >= 0
                     except Exception as e:
                          print("livesstablecount value is negative")

                     try:
                          assert int(val1[2]) >= 0
                     except Exception as e:
                          print("memtablesize value is negative")



                     try:
                          assert float(val1[3]) >= 0
                     except Exception as e:
                          print("latency value is negative")

 
	except Exception as e:
     	    print(str(e))

        #Assertion to check if the values are always zero for the different recorded metrics
        try:
             assert flag_lsstable > 0

        except Exception as e:
             print("All Lsstable entries are zero")

  
        try:
            assert flag_memtsize  > 0
        except Exception as e:
             print("All memtable entries are zero")

        try:
           assert flag_latency > 0
        except Exception as e:
             print("All latency values are zero")


