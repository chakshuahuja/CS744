# Running Spark Application

##### Assumptions: 
1. `Hadoop` and `Spark` are installed on machine with all required configurations (like NameNode, DataNode, Master and slaves updated)
    Verify by running `jps` that it gives the output
    ````
    Worker
    Jps
    SecondaryNameNode
    DataNode
    CoarseGrainedExecutorBackend
    SparkSubmit
    NameNode
    Master
    ````
2. `sbt` is already installed.
If not, follow the steps below to setup the installation
    ###### Installing sbt on Linux based system

    `echo "deb https://dl.bintray.com/sbt/debian/" | sudo tee -a /etc/apt/sources.list.d/sbt.list`
    `sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823`
    `sudo apt-get update`
    `sudo apt-get install sbt`

3. All files are assumed to be on `HDFS`. So be sure by listing files on HDFS `hdfs dfs -ls <path>`. The required files
        * `export.csv` for Simple Sort Application (Part 2) and
        * `enWikiData` directory containing the first 10 `xml` files are on hdfs

4. For our bash scripts, we assume that the Spark bin directory in your machine is `~/spark-2.2.0-bin-hadoop2.7/bin/`. If this is not the case, please change the command to '<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit' in each of the bash scripts.

##### Note:
Output files would get stored on HDFS in home directory `/`
The Spark Assignment for this submission has been :
        *  Programmed in `Scala`
        *  Uses `sbt` interactive build tool to build a jar
        *  Uses `spark-submit` shell script to run spark application

Unzip the `CS744Assignment1.tar.gz` provided by us by running :
`tar -xvzf CS744Assignment1.tar.gz`

For part-2, SORT Change Directory to `CS744Assignment1/SortApp` by `cd CS744Assignment1/SortApp` and executing:
`sbt package` which will build a new jar (Take note that it is run from SortApp directory)


For part-3, PAGERANK Change Directory to `CS744Assignment1/PageRankApp` by `cd CS744Assignment1/PageRankApp` and executing:
`sbt package` which will build a new jar (Take note that it is run from PageRankApp directory)

### Part 1:
Hope the above steps are working fine and you are ready with the setup.

### Part 2:
## Sort 
GO TO SORTAPP DIRECTORY
* Sort using `Data Frame API` for computation:

Code: Present in class `SortDataFrame` in the `SortApp`
Input: CSV File to be sorted (assumed to be on `hdfs`)
Output: CSV File containing sorted(by country and timestamp) records.
    
    ```
    Run using bash script:
    inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file, hdfs://<HOST_IP>:9000/output-directory
    Example: bash run_df.sh 128.104.223.156 hdfs://128.104.223.156:9000/export.csv hdfs://128.104.223.156:9000/sort_rdd.txt

    Running using command:
    
    <PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "SortDataFrame" --master spark://<HOST_IP>:7077 target/scala-2.11/sortapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/export.csv hdfs://<HOST_IP>:9000/dfSortDataSetResult --driver-memory 8G --executor-memory 8G
    ```
    where
    - `<PATH_TO_SPARK_BIN_DIRECTORY>` is the location of Spark bin directory in your machine (would be mostly be `~/spark-2.2.0-bin-hadoop2.7/bin/`),
    - `<HOST_IP>` is the hostname of the `Master` node and you can find in your current machine by running `hostname -i` and
    - `target/scala-2.11/sortapp_2.11-1.0.jar` is the path to jar file
    - `hdfs://<HOST_IP>:9000/export.csv` is the location of input csv file on the HDFS
    - `hdfs://<HOST_IP>:9000/dfSortDataSetResult` is the output file name (can be anything of your choice)

* To run a program that uses RDD for computation

Code: Present in class `SortRDD` in the `SortApp`
Input: CSV File to be sorted (assumed to be on `hdfs`)
Output: CSV File containing sorted(by country and timestamp) records.

    
    ```
    Run using bash script:
    inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file, hdfs://<HOST_IP>:9000/output-directory
    Example: bash run_rdd.sh 128.104.223.156 hdfs://128.104.223.156:9000/export.csv hdfs://128.104.223.156:9000/sort_rdd.txt

    Running using command:
    <PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "SortRDD" --master spark://<HOST_IP>:7077 target/scala-2.11/sortapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/export.csv hdfs://<HOST_IP>:9000/rddSortDataSetResult --driver-memory 8G --executor-memory 8G
    ```

########################################################## Part 3 ##################################################################################33
#### PageRank

GO TO PageRankApp DIRECTORY

Input: Input File or Directory to be sorted (assumed to be on `hdfs`). In case, you are adding the directory path, make sure to add `hdfs://<HOST_IP>:9000/<INPUT_DIR>/*`
Output: The Output File to store the data.

########################################################## Naive Based
```
Run using bash script:
inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file-or-dir, hdfs://<HOST_IP>:9000/output-directory
Example: bash run_naive.sh 128.104.223.156 hdfs://128.104.223.156:9000/berkData hdfs://128.104.223.156:9000/pr_naive

Run using command:
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankNaive" --master spark://<HOST_IP>:7077  target/scala-2.11/pagerankapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<ip_address>:9000/naivePageRankResult --driver-memory 8G --executor-memory 8G
```
########################################################## Partition Based
```
Run using bash script:
inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file-or-dir, hdfs://<HOST_IP>:9000/output-directory, num_partitions
Example: bash run_partition.sh 128.104.223.156 hdfs://128.104.223.156:9000/berkData hdfs://128.104.223.156:9000/pr_partition 30

Run using command:
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankPartition" --master spark://<host IP>:7077  target/scala-2.11/pagerankapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/partitionPageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```
########################################################## Graph Based
```
Run using bash script:
inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file-or-dir, hdfs://<HOST_IP>:9000/output-directory, num_partitions
Example: bash run_graph.sh 128.104.223.156 hdfs://128.104.223.156:9000/berkData hdfs://128.104.223.156:9000/pr_graph 30

Run using command:
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankGraph" --master spark://<host IP>:7077  target/scala-2.11/pagerankapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/graphPageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```

########################################################## Cached Based
```
Run using bash script:
inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file-or-dir, hdfs://<HOST_IP>:9000/output-directory, num_partitions
Example: bash run_cached.sh 128.104.223.156 hdfs://128.104.223.156:9000/berkData hdfs://128.104.223.156:9000/pr_cached 30

Run using command:
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankCached" --master spark://<host IP>:7077  target/scala-2.11/pagerankapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/cachedPageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```
##########################################################  Range Based
```
Run using bash script:
inputs: master-nodeip, hdfs://<HOST_IP>:9000/input-file-or-dir, hdfs://<HOST_IP>:9000/output-directory, num_partitions
Example: bash run_range_partition.sh 128.104.223.156 hdfs://128.104.223.156:9000/berkData hdfs://128.104.223.156:9000/pr_range_partition 30

Run using command:
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankRangePartition" --master spark://<HOST_IP>:7077  target/scala-2.11/pagerankapp_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/rangePageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```
where `<PARTITION_NUMBER>` is any integer value.

##### Kill a Worker process and see the changes. You should trigger the failure to a desired worker VM when the application reaches 50% of its lifetime

In order to test, run the `wait_for_spark_app.py` script using `python3.5 wait_for_spark_app.py`. Make sure the spark session is running at time of running the script so that it is able to track the progress and kill a worker at around 50%.
