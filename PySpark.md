* ## **PySpark**



1. It is python **API for** Apache Spark a powerful Open-source Engine for big Data Processing
2. We can Process Huge Volume of data efficiently across multiple machine in Parallel
3. It Support  batch Processing , real Time Steaming, SQL Quires and Machine Learning



* **SparkSession Vs SparkContext**



1. **SparkContext**



 	- SparkContext is the entry point to Spark's core features.

 	- It connects your program to the Spark cluster and coordinates operations.

 	-  It's responsible for managing job resources, and task execution.



* Why use SparkContext?

 	1. **Low-level Operations**: Provides access to low-level operations and configurations.

 	2. **RDD Manipulation**: Essential for working directly with Resilient Distributed Datasets (RDDs).



rdd  = sc.parallelize(\[1,2,3,4,5,6])



**2.  SparkSession**



 	- SparkSession is a newer, unified entry point to work with both Spark Core and Spark SQL/Data Frames

 	- SparkSession is the modern gateway to Spark. It wraps SparkContext, SQL Context, and Hive Context into 		one



 	SparkContext		StreamingContext		SQLContext

 

 				SparkSession



**RDD (Resilient Distributed Datasets):**  A Fault-Tolerant Abstraction for In-Memory Cluster Computing



RDD is the primary data abstraction in Apache Spark and the core of Spark



* A RDD is a resilient and distributed collection of records.
* As the name suggests isa Resilient (Fault-tolerant) records of data that resides on multiple nodes.



**Advantage of RDD**



* With RDD the creators of Spark managed to hide data partitioning and distribution



**Features of RDD**



1. ln-Memory
2. Lazy-evaluated
3. Immutable
4. Parallel



###### **Creating RDDs and Use Transformation and Actions**



rdd = sc.textFile ("file path")

rdd1 = sc.parallelize(\[10,20,3,0,40,])

rdd2 = sc.parallelize(\[("apple",1),("mango",2)])

rdd3 = sc.parallelize(range(1,11))



Word Count in the file



**collect()** : is the action i.e is trigger that is called for output

**take()**

**reduce() :** it is a action and reduceByKey() is a transformation







###### **PySpark DataFrame and Schemas**





 Distributed collection of data organized into named columns. Similar to a table in a relational database or

 pandas DataFrame.



>>> df\\\_csv = spark.read.option("header",True).csv("C:/Users/Navneet singh/Desktop/Dummy/Scala-Programs/sample.csv",inferSchema=True)

>>>  data = \\\[("abc",30),("xyz",34)]

>>>  columns = \\\["Name","Age"]

>>>  df = spark.CreateDataFrame(data,colums)



###### **Apache Spark DataFrame Operations**



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Testing").master("local\[\*]").getOrCreate()

>>> from pyspark.sql.functions import col,avg,max

>>> df = spark.read.csv("C:/Users/Navneet singh/Downloads/emp.csv",header = True, inferSchema = True)

>>> df.show()

+---+--------+-----+-----------+-------+

| id|    name| age | department| salary|

+---+--------+-----+-----------+-------+

|  1|   Alice|   30|         HR| 4000.0|

|  2|     Bob|   35|        IT | 6000.0|

|  3| Charlie|   25|    Finance| 4500.0|

|  4|   David|   40|         IT| 7000.0|

|  5|     Eva|   29|         HR| 4100.0|

|  6|   Frank|     |    Finance| 3900.0|

|  7|   Grace|   28|           | 4200.0|

|  8|  Hannah|   30|         HR| 4000.0|

|  9|     Ian| NULL|         IT|   NULL|

| 10|     Eva|   29|         HR| 4100.0|

+---+--------+-----+-----------+-------+



>>> df.printSchema()

root

 |-- id: integer (nullable = true)

 |--  name: string (nullable = true)

 |--  age : string (nullable = true)

 |--  department: string (nullable = true)

 |--  salary: double (nullable = true)





>>> df.select("name", "salary").show()

\+ -------- + ------- +

|    name | salary  |

\+ -------- + ------- +

|   Alice  | 4000.0 |

|     Bob  | 6000.0 |

| Charlie | 4500.0 |

|   David | 7000.0 |

|     Eva  | 4100.0 |

|   Frank | 3900.0 |

|   Grace | 4200.0 |

|  Hannah| 4000.0|

|     Ian   |   NULL|

|     Eva  | 4100.0  |

\+ -------- + -------    +



>>> df.filter(col(" age ")>30).show()

+---+------+-----+-----------+-------+

| id|  name| age | department| salary|

+---+------+-----+-----------+-------+

|  2|   Bob|   35|        IT | 6000.0|

|  4| David|   40|         IT| 7000.0|

+---+------+-----+-----------+-------+





>>> df = df.withColumn("bonus",col(" salary")\\\*0.1).show()

+---+--------+-----+-----------+-------+-----+

| id|    name| age | department| salary|bonus|

+---+--------+-----+-----------+-------+-----+

|  1|   Alice|   30|         HR| 4000.0|  0.0|

|  2|     Bob|   35|        IT | 6000.0|  0.0|

|  3| Charlie|   25|    Finance| 4500.0|  0.0|

|  4|   David|   40|         IT| 7000.0|  0.0|

|  5|     Eva|   29|         HR| 4100.0|  0.0|

|  6|   Frank|     |    Finance| 3900.0|  0.0|

|  7|   Grace|   28|           | 4200.0|  0.0|

|  8|  Hannah|   30|         HR| 4000.0|  0.0|

|  9|     Ian| NULL|         IT|   NULL| NULL|

| 10|     Eva|   29|         HR| 4100.0|  0.0|

+---+--------+-----+-----------+-------+-----+

 df = df.withColumnRenamed(" salary ","monthly\_salary")

>>>  df = df.drop()

>> >  df.orderBy(col("salary").desc()).show()

>>>  df.groupBy("department").agg(avg("salary").alias("avg\\\_salary"), max("salary").alias("max\\\_salary")).show()



>>>  df.select("department").distinct().show()



>>>  df.groupBy("department").agg(avg("salary").alias("avg\\\_salary"), max("salary").alias("max\\\_salary")).show()

>>>    df.na.fill({"age":30,"department":"Unkown","salary":3500}).show()

>>>  df.describe().show()



>>> df.select(col("salary").cast("string")).printSchema()

>>> df.write.csv("filepath",header=True)

###### 

###### **Handling Missing values**



.isNull()

