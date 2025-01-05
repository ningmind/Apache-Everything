import csv
from io import StringIO
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.sql.functions import *
import pandas as pd
import json

# Documentation: https://spark.apache.org/docs/latest/rdd-programming-guide.html
# Below is to solve [PYTHON_VERSION_MISMATCH] issue
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def init_spark():
    # spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
    # sc = spark.sparkContext
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf = conf)
    return sc

def init_spark_session():
    spark = SparkSession.builder.master("local[4]").getOrCreate()
    return spark

def loadCSV(fileName):
    input = StringIO(fileName[1])
    reader = csv.DictReader(input)
    # reader = csv.DictReader(input, fieldnames = ["name", "age", "job"]) # use this if no title in csv
    return reader

def extractCallSigns(line):
    global blankLines
    if (line == ""):
        blankLines += 1
    return line.split(" ")

def main():
    sc = init_spark()
    spark = init_spark_session()

    # Chapter 3 Programming with RDDs (Resilient Distributed Datasets)
    # 1. Creating RDDs
    lines = sc.textFile("SparkTest.md")
    wordsRDD = sc.parallelize(["pandas", "i like pandas"])
    numsRDD = sc.parallelize([1,2,3,4])
    oddNumsRDD = sc.parallelize([1,3,5,7,9])

    # 2. RDD Operations - Transformations
    # 2.1 filter
    testsRDD = lines.filter(lambda line: "test" in line)
    print(testsRDD.collect()) # ['This is a test file.']
    errorsRDD = lines.filter(lambda x: "error" in x)
    print(errorsRDD.collect()) # ['This is the error line.']
    warningsRDD = lines.filter(lambda x: "warning" in x)
    print(warningsRDD.collect()) # ['This is the warning line.']
    # 2.2 union
    badLinesRDD = errorsRDD.union(warningsRDD)
    print(badLinesRDD.collect()) # ['This is the error line.', 'This is the warning line.']
    # 2.3 map - pass each element of the source through a function
    squaresRDD = numsRDD.map(lambda x: x*x)
    print(squaresRDD.collect()) # [1, 4, 9, 16]
    # 2.4 flatmap - each input item can be mapped to 0 or more output items
    wordsSplitRDD = wordsRDD.flatMap(lambda line: line.split(" "))
    print(wordsSplitRDD.collect()) # ['pandas', 'i', 'like', 'pandas']
    # 2.5 intersection
    intersectionRDD = numsRDD.intersection(oddNumsRDD)
    print(intersectionRDD.collect()) # [1, 3]
    # 2.6 distinct
    numsAllRDDs = numsRDD.union(oddNumsRDD).distinct()
    print(numsAllRDDs.collect()) # [2, 4, 1, 3, 5, 7, 9]

    # 3. RDD Operations - Actions
    # 3.1 count
    print(lines.count()) # 4 - count the number of items in RDD
    # 3.2 collect
    print(numsRDD.collect()) # [1, 2, 3, 4]
    # 3.3 take
    print(lines.take(2)) # ['Apache Spark', 'This is a test file.']
    # 3.4 first - same as take(1)
    print(lines.first()) # Apache Spark
    # 3.5 reduce
    print(numsRDD.reduce(lambda x, y: x + y)) # 10
    # 3.6 aggregate
    seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
    combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print(numsRDD.aggregate((0, 0), seqOp, combOp)) # (10, 4)
    ## Explanation
        # (1) First partition [1,2] with seqOp, processing first element, y = 1
        # At the beginning, x[0] = 0, x[1] = 0 due to zeroValue
        # local result (new x) = (x[0] + y, x[1] + 1) = (0 + 1, 0 + 1) = (1, 1)
        # (2) First partition [1,2] with seqOp, processing second element, y = 2
        # x = (x[0] + y, x[1] + 1) = (1 + 2, 1 + 1) = (3, 2)
        # (3) Second partition [3,4] with seqOp, processing third element, y = 3
        # x[0] = 0, x[1] = 0 back to the zeroValue
        # x = (x[0] + y, x[1] + 1) = (0 + 3, 0 + 1) = (3, 1)
        # (4) Second partition [3,4] with seqOp, processing fourth element, y = 4
        # x = (x[0] + y, x[1] + 1) = (3 + 4, 1 + 1) = (7, 2)
        # (5) Apply combOp to each local result
        # Now one local result = (3, 2), another local result = (7, 2)
        # (x[0] + y[0], x[1] + y[1]) = (3 + 7, 2 + 2) = (10, 4)

    # Chapter 4 Working with Key/Value Pairs
    # 1. Creating RDD with key/value pairs
    pairsRDD1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
    pairsRDD2 = sc.parallelize([("c", 6), ("b", 5), ("a", 4), ("a", 1)])
    pairsRDD3 = sc.parallelize([('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5),
                                ('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)])
    pairs = lines.map(lambda x: (x.split(" ")[0], x)) # use first word as key

    # 2. RDD Operations - Transformations
    # 2.1 groupbykey
    groupCountKeyRDD = pairsRDD1.groupByKey().mapValues(len) # count key
    print(groupCountKeyRDD.collect()) # [('a', 1), ('b', 1), ('c', 1)]
    groupCombineKeyRDD = pairsRDD1.groupByKey().mapValues(list)
    print(groupCombineKeyRDD.collect()) # [('a', [1]), ('b', [2]), ('c', [3])]
    # 2.2 reducebykey: merge the values for each key
    sumValueRDD = pairsRDD2.reduceByKey(add)
    print(sumValueRDD.collect()) # [('c', 6), ('b', 5), ('a', 5)]
    reduceKeyRDD = pairsRDD2.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print(reduceKeyRDD.collect()) # [('c', (6, 1)), ('b', (5, 1)), ('a', (5, 2))]
    # 2.3 aggregatebykey: aggregate the values for each key using zero value
    aggregateKeyRDD = pairsRDD2.aggregateByKey((0, 0), seqOp, combOp)
    print(sorted(aggregateKeyRDD.collect())) # [('a', (5, 2)), ('b', (5, 1)), ('c', (6, 1))]
    ## Explanation
        # (1) Processing ("a", 4) with seqOp, y = 4
        # x = (x[0] + y, x[1] + 1) = (0 + 4, 0 + 1) = (4, 1)
        # (2) Processing ("a", 1) with seqOp, y = 1
        # x = (x[0] + y, x[1] + 1) = (0 + 1, 0 + 1) = (1, 1) still use zero value
        # (3) Processing with combOp for a
        # (x[0] + y[0], x[1] + y[1]) = (4 + 1, 1 + 1) = (5, 2)
        # (4) More straightforward for element b and c
    # 2.4 combinebykey
    combineKeyRDD = pairsRDD1.combineByKey((lambda x: (x, 4)),
                                      (lambda x, y: (x[0] + y[0], x[1] + y[1])),
                                      (lambda x, y: (x[0] + y[0], x[1] + y[1])))
    print(combineKeyRDD.collect()) # [('a', (1, 4)), ('b', (2, 4)), ('c', (3, 4))]
    # 2.5 sortbykey
    sortKeyRDD = pairsRDD3.sortByKey(True, 1)
    print(sortKeyRDD.collect()) # [('1', 3), ('2', 5), ('Mary', 1), ('a', 1), ('a', 3),
                                # ('b', 2), ('d', 4), ('had', 2), ('lamb', 5), ('little', 4)]
    sortKeyFuncRDD = pairsRDD3.sortByKey(True, 3, keyfunc=lambda k: k.lower())
    print(sortKeyFuncRDD.collect()) # [('1', 3), ('2', 5), ('a', 1), ('a', 3), ('b', 2),
                                    # ('d', 4), ('had', 2), ('lamb', 5), ('little', 4), ('Mary', 1)]
    # 2.6 join
    joinRDD = pairsRDD1.join(pairsRDD2)
    print(joinRDD.collect()) # [('b', (2, 5)), ('c', (3, 6)), ('a', (1, 4)), ('a', (1, 1))]
    # 2.7 cogroup
    coGroup = [(x, tuple(map(list, y))) for x, y in sorted(list(pairsRDD1.cogroup(pairsRDD2).collect()))]
    print(coGroup) # [('a', ([1], [4, 1])), ('b', ([2], [5])), ('c', ([3], [6]))]

    # 3. RDD Operations - Actions
    # 3.1 countbykey
    print(pairsRDD2.countByKey().items()) # dict_items([('c', 1), ('b', 1), ('a', 2)])
    # 3.2 collectasmap
    print(pairsRDD1.collectAsMap()) # {'a': 1, 'b': 2, 'c': 3}
    print(combineKeyRDD.map(lambda xy: (xy[0], xy[1][0] / xy[1][1])).collectAsMap()) # {'a': 0.25, 'b': 0.5, 'c': 0.75}
    # 3.3 lookup
    print(pairsRDD3.lookup("Mary")) # [1]

    # Chapter 5 Loading and Saving Your Data
    # 5.1 Text Files
    text_output = sc.textFile("SparkTest.md")
    print(text_output.collect()) # ['Apache Spark', 'This is a test file.', 'This is the error line.', 'This is the warning line.']
    # text_output.saveAsTextFile("SparkTestOutput.md")
    # 5.2 JSON Files
    json_output = spark.read.json("SparkTest.json")
    # print(json_output.show())
    # 5.3 CSV Files
    csv_output = sc.wholeTextFiles("SparkTest.csv").flatMap(loadCSV)
    print(csv_output.collect()) # [{'name': 'Jorge', 'age': '30', 'job': 'Developer'}, {'name': 'Bob', 'age': '32', 'job': 'Developer'}]

    # 5.4 Spark SQL
    hive = SparkSession.builder.enableHiveSupport().getOrCreate() # replacement of HiveContext(sc)
    # rows = hive.sql("SELECT name, age FROM users") # need installation and configuration of Hive
    # print(rows.first().name) # need installation and configuration of Hive
    tweets = hive.read.json("tweets.json") # replacement of .jsonFile()
    tweets.createOrReplaceTempView("tweets") # replacement of registerTempTable()
    results = hive.sql("SELECT user.name, text FROM tweets")
    print(results.collect())

    # Chapter 6 Advanced Spark Programming
    global blankLines
    blankLines = sc.accumulator(0) # Accumulators
    print(blankLines)
    callSigns = lines.flatMap(extractCallSigns)
    print(callSigns.count())
    print("Blank lines: %d" % blankLines.value)

    # Chapter 7 Running on a Cluster
    # 7.1 Spark Distributed Mode
    ## Driver: central coordinator, converting a user program into tasks and scheduling tasks on executors
    ## Executors: worker processes, running individual tasks in a given Spark job

    # 7.2 Cluster Managers
    ## Standalone Mode: run Spark by itself on a set of machines
    ## Hadoop YARN: allow diverse data processing frameworks to run on a shared resource poll, installed on HDFS node


if __name__ == '__main__':
    main()