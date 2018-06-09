import subprocess
import time
import os
from db2connect import dbconnect

#Method to check if cassandra is running
def check_cassandra_running():
     process_name = "cassandra"
     tmp = os.popen("ps -Af").read()
     cassandra_service_restart = "echo sudo service cassandra restart"
     
     if process_name not in tmp[:]:
         print("Cassandra is not running. Restarting Cassandra service")
         subprocess.call(cassandra_service_restart, shell=True)
     else:
         print("Cassandra is running")

#Runs the cassandra stress tool and recording tool
def run_stress_recording():

    p=subprocess.Popen(["python","metrics_record.py"])
    cstress_tool="cassandra-stress write n=10000000 -rate threads=5"
    subprocess.call(cstress_tool,shell=True)
    p.kill()


#Connects to the Cassandra database and records the entries 
def record_data_cassandra():
    db_obj=dbconnect()
    db_obj.insert_db()


def main():
    check_cassandra_running()
    run_stress_recording()
    record_data_cassandra()

if __name__=="__main__":
     main()
