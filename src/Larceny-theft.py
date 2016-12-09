# sub-Crime associated with particular crime (LARCENY-THEFT)
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext

input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

crime=sqlContext.read.parquet(input).cache()
crime.registerTempTable('Crime')


Crime=sqlContext.sql('''select IncidntNum,count(IncidntNum) as inc_num_cnt
                        from Crime group by IncidntNum having count(IncidntNum)>1 
                    ''').cache()
Crime.registerTempTable('Crime_Frequency')

Crime_Frequency=sqlContext.sql('''select distinct CF.IncidntNum from Crime_Frequency CF,Crime C
                                  Where CF.IncidntNum=C.IncidntNum and C.Category='LARCENY-THEFT'  ''')
Crime_Frequency.registerTempTable('Crime_Vehicle_Theft')

# Extracting crimes that are associated with LARCENY-THEFT
Crime_Vehicle_Theft=sqlContext.sql('''select  CV.IncidntNum,C.Category,count(Category) as cnt from Crime_Vehicle_Theft CV,Crime C
                                      where CV.IncidntNum=C.IncidntNum and C.Category not in ('LARCENY-THEFT','OTHER OFFENSES')
                                      group by CV.IncidntNum,C.Category having count(Category)>1''')
Crime_Vehicle_Theft.registerTempTable('Crime_VT_Frequency')

Crime_VT_Freq=sqlContext.sql('''select  Category ,sum(cnt) as t_cnt from Crime_VT_Frequency group by Category''')

Crime_VT_Freq.show()
#Crime_VT_Freq.coalesce(1).write.mode('append').json("jsonpath")
Crime_VT_Freq.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('LARCENY-THEFT')