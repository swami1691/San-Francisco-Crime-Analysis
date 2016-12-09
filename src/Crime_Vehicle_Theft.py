# sub-Crime associated with particular crime (VEHICLE THEFT)
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext

input = sys.argv[1]

conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crime=sqlContext.read.parquet(input).cache()
crime.registerTempTable('Crime')

#taking only multiple crimes
Crime=sqlContext.sql('''select IncidntNum,count(IncidntNum) as inc_num_cnt
                        from Crime group by IncidntNum having count(IncidntNum)>1 
                    ''').cache()
Crime.registerTempTable('Crime_Frequency')

# narrowing search pertaining to multiple crimes.

Crime_Frequency=sqlContext.sql('''select distinct CF.IncidntNum from Crime_Frequency CF,Crime C
                                  Where CF.IncidntNum=C.IncidntNum and C.Category='VEHICLE THEFT'  ''')
Crime_Frequency.registerTempTable('Crime_Vehicle_Theft')

#Filtering out crimes associated with vehicle theft
Crime_Vehicle_Theft=sqlContext.sql('''select  CV.IncidntNum,C.Category,count(Category) as cnt from Crime_Vehicle_Theft CV,Crime C
                                      where CV.IncidntNum=C.IncidntNum and C.Category not in ('VEHICLE THEFT','OTHER OFFENSES')
                                      group by CV.IncidntNum,C.Category having count(Category)>1''')
Crime_Vehicle_Theft.registerTempTable('Crime_VT_Frequency')

# taking count associated with crimes
Crime_VT_Freq=sqlContext.sql('''select  Category ,sum(cnt) as t_cnt from Crime_VT_Frequency group by Category''')


#Crime_VT_Freq.coalesce(1).write.mode('append').json("jsonpath")
Crime_VT_Freq.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('VEHICLE_THEFT')