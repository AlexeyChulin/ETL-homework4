import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()

# NB: needs mysqlclient to be installed for connecting to mysql. Run `pip install mysqlclient`. 
# If pip fails to install mysqlclient, try this:
# https://stackoverflow.com/questions/76585758/mysqlclient-cannot-install-via-pip-cannot-find-pkg-config-name-in-ubuntu
engine = create_engine("mysql://alexey:astron93@localhost:3306/spark")
con = engine.connect()
print(con) 
# sql.execute('create table if not exists test(a int, b int) engine=InnoDB', con) # just test of connection
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("Hi") \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2") \
        .getOrCreate()
tables = ['credit_plan1', 'credit_plan2', 'credit_plan3']
#sheets = ['Лист1', 'Лист2', 'Лист3']

for table in tables:
    sql.execute(f"""drop table if exists spark.`{table}`""", con)
    sql.execute(f"""CREATE TABLE if not exists spark.`{table}` (
        `№` INT(10) NULL DEFAULT NULL,
    	`Месяц` DATE NULL DEFAULT NULL,
    	`Сумма платежа` FLOAT NULL DEFAULT NULL,
    	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
    	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
    	`Остаток долга` FLOAT NULL DEFAULT NULL,
    	`проценты` FLOAT NULL DEFAULT NULL,
    	`долг` FLOAT NULL DEFAULT NULL
    )
    COLLATE='utf8mb4_0900_ai_ci'
    ENGINE=InnoDB""", con)

from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1

debt_colors = ['red', 'blue', 'green']
interest_colors = ['cyan', 'yellow', 'magenta']
""" for i in range(3):
    print(debt_colors[i])
    print(interest_colors[i]) """

for i in range(3):
    w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df0 = pd.read_excel('s4_21.xlsx', sheet_name=i)
    print(df0.head())
    df1 = spark.createDataFrame(df0).limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w)) 
    # throws SparkClassNotFoundException: [DATA_SOURCE_NOT_FOUND] Failed to find the data source: excel
    """ df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	    .option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))  """
    df1.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=alexey&password=astron93")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", tables[i])\
        .mode("append").save()
    df2 = df1.toPandas()
    # Get current axis 
    ax = plt.gca()
    ax.ticklabel_format(style='plain')
    # bar plot
    df2.plot(kind='line', 
        x='№', 
        y='долг', 
        color=debt_colors[i], ax=ax)
    df2.plot(kind='line', 
        x='№', 
        y='проценты', 
        color=interest_colors[i], ax=ax)
    # set the title 
    plt.title('Выплаты')
    plt.grid ( True )
    ax.set(xlabel=None)
    i += 1
# save and show the plot 
plt.savefig('homework4.png')
plt.show() 
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
