# District - particular Time -Crime -Max Count 
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext

input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#loading data into parquet file
crime=sqlContext.read.parquet(input).cache()
crime.registerTempTable('Crime')

Crime_Month=sqlContext.sql('''select Month,count(Category)
                        from Crime   group by Month
                    ''')

Crime_Time=sqlContext.sql('''select substr(Time,1,2),count(Category)
                        from Crime group by  substr(Time,1,2)
                    ''')

# caching the output as it will be required 
Crime_Time_Category=sqlContext.sql('''select substr(Time,1,2) as Hour,PdDistrict,Category,count(Category) as cat_cnt
                        from Crime where Category<>'OTHER OFFENSES' group by  substr(Time,1,2),PdDistrict,Category
                    ''').cache()

Crime_Time_Category.registerTempTable('Crime_Time_Category')


Crime_Time_Category_Max=sqlContext.sql('''select Hour,PdDistrict,Max(cat_cnt) as cat_cnt from Crime_Time_Category 
                                            where Category<>'OTHER OFFENSES' group by Hour,PdDistrict
                                       ''')

Crime_Time_Category_Max.registerTempTable('Crime_Time_Category_Max')

#calcluating distict wise max crime occured in a particular time

Crime_Time_Category_Max_cnt=sqlContext.sql('''select CTCM.Hour,CTC.PdDistrict,CTC.Category,CTCM.cat_cnt as cat_cnt
                                            from Crime_Time_Category CTC,
                                          Crime_Time_Category_Max CTCM where CTC.Hour=CTCM.Hour 
                                          and CTC.PdDistrict=CTCM.PdDistrict
                                          and CTC.cat_cnt=CTCM.cat_cnt order by  Hour
                                       ''')
# coalescing the output contents and its safe to do as it will not affect parallel operation much.
Crime_Time_Category_Max_cnt.coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('District_Time_Crime')