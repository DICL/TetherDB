## More Experiments


<br>
<br>
<br>


### [mixgraph (variable-sized values, Varying arrival rate)]

<img src="mixgraph_light.png" width="500"/>

#### [Light]

<img src="mixgraph_heavy.png" width="500"/>

#### [Heavy]

<br>
<br>

### [latency-throughput (Different thread numbers)]

<img src="latency-throughput.png" width="500"/>

SpanDB does not support workload D.<br>
SpanDB crashes during the YCSB Load workload, which involves inserting dozens of gigabytes of data. <br>
To work around this and measure write performance, we followed the approach of the SpanDB authors. <br>
First, we load 200 GB of data using vanilla RocksDB. Then, we open the database with SpanDB and run a 100% write workload, performing an additional 20 million writes. <br>
This workload is called "PeakWR." <br>
<br>
<br>

### [Throughput varying Zipf Factor]

<img src="zipf.png" width="700"/>

<br>
<br>

### [Impact of Designs on Execution Time and Write Stall]
![write stall](write_stall.png)
