import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructField, StructType,IntegerType,FloatType
from pyspark.sql import functions as F
sc = SparkContext('local', 'spark_project')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

schemaString = "Country,Year,Total,Coal,Oil,Gas,Cement,Flaring,Other,PerCapita"
fields = []
for field in schemaString.split(","):
    if field == 'Country':
        a = StructField(field, StringType(), True)
    elif field == 'Year':
        a = StructField(field,IntegerType(),True)
    else:
        a = StructField(field,FloatType(),True)
    fields.append(a)

schema = StructType(fields)
CO2Rdd1 = sc.textFile('/user/xmw/dataset.csv')
CO2Rdd = CO2Rdd1.map(lambda x:x.split("\t")).map(lambda p: Row(p[0],int(p[1]),float(p[2]),float(p[3]),float(p[4]),float(p[5]),float(p[6]),float(p[7]),float(p[8]),float(p[9])))
mdf = spark.createDataFrame(CO2Rdd, schema)
mdf.createOrReplaceTempView("usInfo")
mdf.show()

mdf.printSchema()


# 可视化
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


# 总排放量前5国家柱状图
top5_total = mdf.groupBy("Country") \
                .agg(F.sum("Total").alias("TotalEmissions")) \
                .orderBy(F.desc("TotalEmissions")) \
                .limit(5)

top5_pd = top5_total.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(top5_pd['Country'], top5_pd['TotalEmissions'], color='steelblue')
plt.title('Top 5 Countries by Total CO₂ Emissions (All Years)', fontsize=14)
plt.xlabel('Country')
plt.ylabel('Total CO₂ Emissions (Million Metric Tons)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/hadoop/PycharmProjects/CO2Project1/top5_total.png')
plt.show()

# 2000年后排放饼图
recent_df = mdf.filter(mdf.Year >= 2000)
country_recent = recent_df.groupBy("Country") \
                          .agg(F.sum("Total").alias("TotalEmissions")) \
                          .orderBy(F.desc("TotalEmissions"))

country_pd = country_recent.toPandas()
topN = 8
top_countries = country_pd.head(topN).copy()
others_sum = country_pd.iloc[topN:]['TotalEmissions'].sum()

if others_sum > 0:
    others_row = pd.DataFrame([{'Country': 'Others', 'TotalEmissions': others_sum}])
    plot_data = pd.concat([top_countries, others_row], ignore_index=True)
else:
    plot_data = top_countries

plt.figure(figsize=(10, 8))
plt.pie(
    plot_data['TotalEmissions'],
    labels=plot_data['Country'],
    autopct='%1.1f%%',
    startangle=90,
    textprops={'fontsize': 10}
)
plt.title('CO₂ Emissions by Country (Year ≥ 2000)', fontsize=14)
plt.axis('equal')
plt.tight_layout()
plt.savefig('/home/hadoop/PycharmProjects/CO2Project1/emissions_2000+.png')
plt.show()