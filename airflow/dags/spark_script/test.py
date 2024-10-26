from spark_hadoop_operations import get_sparkSession, read_mongoDB, write_HDFS

if __name__ == "__main__":
    with get_sparkSession(appName = 'huynhthuan') as spark:
        data = read_mongoDB(spark, 'testdb', 'testcollection')
        data.printSchema()
        print(data.count())
        data.show()
        print(type(data))
        write_HDFS(spark, data, 'testtable', 'parquet')
        