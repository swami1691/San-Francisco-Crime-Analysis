import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext

input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# loading Crime data from parquet
time=sqlContext.read.parquet(input).cache()
time.registerTempTable('Time_table')

#
Time_table=sqlContext.sql('''select DayOfWeek,SUBSTR(Time,1,2) as hour, count(Category) as cnt from Time_table group by DayOfWeek,SUBSTR(Time,1,2)''').cache()

Time_table.registerTempTable('Time_cnt')

Time_Max=sqlContext.sql('''select DayOfWeek,max(cnt) as cnt from Time_cnt group by DayOfWeek ''')
Time_Max.registerTempTable('Time_max')

# Day wise - hour wise - Max crime calculation
Time_week_Max=sqlContext.sql('''select TM.DayOfWeek,TC.hour,TM.cnt from Time_cnt TC,Time_max TM where TC.DayOfWeek=TM.DayOfWeek
and TC.cnt=TM.cnt ''')

#coalescing the final output as it a safe operation to do on our final results-it doesnt affect parallel operation much
Time_week_Max.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('Day_MaxTime_Crime')