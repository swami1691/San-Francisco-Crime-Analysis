# for front-end purposes displaying 2016 year with most number of crimes
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext


input = sys.argv[1]
conf = SparkConf().setAppName('Crime Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
crime=sqlContext.read.parquet(input)
crime.registerTempTable('Crime')

District_WeekCrime=sqlContext.sql('select Year,count(Category) w_cnt from Crime  where Year=2016 and Month=11 and Day>=3 group by Year order by w_cnt desc')
District_WeekCrime.registerTempTable('Week_2016')

#District_WeekCrime=sqlContext.sql('select Year,max(cnt) cnt from Week_2016 Group by Year ')
District_WeekCrime.show()

District_MonthCrime=sqlContext.sql('select Year,count(Category) m_cnt from Crime  where Year=2016 and Month=11 Group by Year order by m_cnt desc')
District_MonthCrime.registerTempTable('Month_2016')
District_MonthCrime.show()

District_YearCrime=sqlContext.sql('select Year,count(Category) y_cnt from Crime  where Year=2016  Group by Year order by y_cnt desc')
District_YearCrime.registerTempTable('Year_2016')
District_YearCrime.show()

District_Overall=sqlContext.sql('select w_cnt as week_crime_count,m_cnt as month_crime_count,y_cnt as Year_crime_count from Week_2016 W,Month_2016 M,Year_2016 Y where W.Year=M.Year and M.Year=Y.Year')
District_Overall.coalesce(1).write.mode('append').json("Crimes_Overall")


#Reported_District_Highest_Crime for front end purpose

District_WeekCrime=sqlContext.sql('select PdDistrict,count(Category) w_cnt from Crime  where Year=2016 and Month=11 and Day>=3 group by PdDistrict order by w_cnt desc')
District_WeekCrime.registerTempTable('Week_2016')

#District_WeekCrime=sqlContext.sql('select Year,max(cnt) cnt from Week_2016 Group by Year ')
District_WeekCrime.show()

District_MonthCrime=sqlContext.sql('select PdDistrict,count(Category) m_cnt from Crime  where Year=2016 and Month=11 Group by PdDistrict order by m_cnt desc')
District_MonthCrime.registerTempTable('Month_2016')
District_MonthCrime.show()

District_YearCrime=sqlContext.sql('select PdDistrict,count(Category) y_cnt from Crime  where Year=2016  Group by PdDistrict order by y_cnt desc')
District_YearCrime.registerTempTable('Year_2016')
District_YearCrime.show()

District_Overall=sqlContext.sql('select W.PdDistrict as Week_District,w_cnt as week_crime_count,M.PdDistrict as Month_District,m_cnt as month_crime_count,Y.PdDistrict as Year_District,y_cnt as Year_crime_count from Week_2016 W,Month_2016 M,Year_2016 Y where W.PdDistrict=M.PdDistrict and M.PdDistrict=Y.PdDistrict order by w_cnt desc ,m_cnt desc ,y_cnt desc limit 1')
District_Overall.show()
District_Overall.coalesce(1).write.mode('append').json("Reported_District_Highest_Crime")

# Reported_Category_Highest_Crime


Category_WeekCrime=sqlContext.sql('select Category,count(Category) w_cnt from Crime  where Year=2016 and Month=11 and Day>=3 group by Category order by w_cnt desc limit 1 ')
Category_WeekCrime.registerTempTable('Week_2016')

District_WeekCrime.show()

Category_MonthCrime=sqlContext.sql('select Category,count(Category) m_cnt from Crime  where Year=2016 and Month=11 Group by Category order by m_cnt desc limit 1')
Category_MonthCrime.registerTempTable('Month_2016')
Category_MonthCrime.show()

Category_YearCrime=sqlContext.sql('select Category,count(Category) y_cnt from Crime  where Year=2016  Group by Category order by y_cnt desc limit 1')
Category_YearCrime.registerTempTable('Year_2016')
Category_YearCrime.show()

Category_Overall=sqlContext.sql('select W.Category as Week_Category,w_cnt as week_crime_count,M.Category as Month_Category,m_cnt as month_crime_count,Y.Category as Year_Category,y_cnt as Year_crime_count from Week_2016 W,Month_2016 M,Year_2016 Y where W.Category=M.Category and M.Category=Y.Category order by w_cnt desc ,m_cnt desc ,y_cnt desc limit 1')
Category_Overall.show()
Category_Overall.coalesce(1).write.mode('append').json("Reported_Category_Highest_Crime")