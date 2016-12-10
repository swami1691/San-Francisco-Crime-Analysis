spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Blue_White_Collar.py  /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Larceny-theft.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster Front_end_results.py /user/chandras/SFPD_parquet

spark-submit --packages com.databricks:spark-csv_2.11:1.3.0  --master=yarn-cluster DayTime_max_Crime.py /user/chandras/SFPD_parquet
