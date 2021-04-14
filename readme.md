

## Getting the environment up and running

Execute the `run.sh` script file.  This runs the docker compose file which creates a three node MongoDB cluster, configures it as a replica set on prt 27017. Spark is also deployed in this environment with a master node located at port 8080 and two worker nodes listening on ports 8081 and 8082 respectively.  The MongoDB cluster will be used for both reading data into Spark and writing data from Spark back into MongoDB.

Note: You may have to mark the .SH file as runnable with the `chmod` command i.e. `chmod +x run.sh`

## run mongorestore 
```
mongorestore --db classwalletTest dump/classwallet
```

To start, this will create the SparkSession and set the environment to use our local MongoDB cluster.

```
from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.input.uri","mongodb://mongo1:27017/classwalletTest.users?replicaSet=rs0").\
        config("spark.mongodb.output.uri","mongodb://mongo1:27017/classwalletTest.users?replicaSet=rs0").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()
```
Next load the dataframes from MongoDB
```
df = spark.read.format("mongo").load()
```
Let’s verify the data was loaded by looking at the schema:
```
df.printSchema()
```

