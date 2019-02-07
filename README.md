
# CS744

[Problem Set](http://pages.cs.wisc.edu/~akella/CS744/S19/assignment1_html/assignment1.html)

### Using parallel-ssh and parallel-scp

```
parallel-ssh -i -h slaves -O StrictHostKeyChecking=no hostname
```
where `slaves` is the filename containing all the IPs to which ssh needs to be performed and `hostname` is the command to be executed in this case
 
```
parallel-scp -h <file_name_containing_all_IPs> <local> <remote>
```
For example, copying `hadoop-env.sh` from master node to all slave nodes
```
parallel-scp -h slaves hadoop-env.sh ~/hadoop-2.7.6/etc/hadoop/hadoop-env.sh
```

## Spark

Spark Jobs listed at http://<namenode_IP>:4040/jobs/


In HDFS Cluster,

Checking status of HDFS Cluster : http://<namenode_IP>:50070/dfshealth.html#tab-overview

```
hdfs dfs -put <filename> /
```