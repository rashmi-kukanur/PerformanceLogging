
import subprocess
import time
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.ion()
ssval_y_axis = []
memtable_y_axis = []
latency_y_axis = []
time_x_axis = []
     
def record_values():
#Using JMXTerm capture the following Cassandra metrics and plot it to graph and save the output to CSV
     record_ssval="echo get -s -b org.apache.cassandra.metrics:name=LiveSSTableCount,type=ColumnFamily Value | java -jar /usr/bin/jmxterm-1.0-alpha-4-uber.jar -l 127.0.0.1:7199 -n -v silent"
 
    record_memval="echo get -s -b org.apache.cassandra.metrics:name=AllMemtablesLiveDataSize,type=Table Value | java -jar /usr/bin/jmxterm-1.0-alpha-4-uber.jar -l 127.0.0.1:7199 -n -v silent"   

     record_latency_95per="echo get -s -b org.apache.cassandra.metrics:name=Latency,scope=Write,type=ClientRequest 95thPercentile Value | java -jar /usr/bin/jmxterm-1.0-alpha-4-uber.jar -l 127.0.0.1:7199 -n -v silent"

     out_time = time.time()
     out_ssval = subprocess.check_output(record_ssval, shell=True)
     out_memtable = subprocess.check_output(record_memval, shell=True)
     out_latency = subprocess.check_output(record_latency_95per, shell=True)

     ssval_y_axis.append(out_ssval)
     memtable_y_axis.append(out_memtable)
     latency_y_axis.append(out_latency)
     time_x_axis.append(out_time)
     plt.clf()
     plt.scatter(time_x_axis,ssval_y_axis)
     plt.plot(time_x_axis,ssval_y_axis)
     plt.savefig('livesstablefig')
     plt.draw()

     plt.clf()
     plt.scatter(time_x_axis,memtable_y_axis)
     plt.plot(time_x_axis,memtable_y_axis)
     plt.savefig('memtablefig')
     plt.draw()

     plt.clf()
     plt.scatter(time_x_axis,latency_y_axis)
     plt.plot(time_x_axis,latency_y_axis)
     plt.savefig('latencyfig')
     plt.draw()

     row = str(out_time).strip("\n") + "," + out_ssval.strip("\n") + "," + out_memtable.strip("\n") + "," + out_latency.strip("\n") + "\n"


     try:
          with open("jmxmetrics.csv", "a") as fh:
               fh.write(row)

     except Exception as e:
          print(str(e))

    
  
def main():
    while(True):
         record_values()
    

if __name__=="__main__":
    main()
  
