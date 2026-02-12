import findspark
findspark.init()
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import when
import os
import jieba
import re
import pandas as pd


# 初始化Spark和加载数据
def initialize(txt_file):
    spark = SparkSession.builder \
        .appName("BilibiliWeeklyAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def safe_parse_line(line):
        line = line.strip()
        if not line:
            return None
        fields = line.split('\t')
        if len(fields) != 14:
            return None
        try:
            return Row(
                up=fields[0],
                time=fields[1],
                title=fields[2],
                desc=fields[3],
                view=int(fields[4]),
                danmaku=int(fields[5]),
                reply=int(fields[6]),
                favorite=int(fields[7]),
                coin=int(fields[8]),
                share=int(fields[9]),
                like=int(fields[10]),
                rcmd_reason=fields[11],
                tname=fields[12],
                his_rank=int(fields[13])
            )
        except (ValueError, IndexError):
            return None

    rdd = spark.sparkContext.textFile(txt_file) \
        .map(safe_parse_line) \
        .filter(lambda x: x is not None)

    schema = StructType([
        StructField("up", StringType(), True),
        StructField("time", StringType(), True),
        StructField("title", StringType(), True),
        StructField("desc", StringType(), True),
        StructField("view", IntegerType(), True),
        StructField("danmaku", IntegerType(), True),
        StructField("reply", IntegerType(), True),
        StructField("favorite", IntegerType(), True),
        StructField("coin", IntegerType(), True),
        StructField("share", IntegerType(), True),
        StructField("like", IntegerType(), True),
        StructField("rcmd_reason", StringType(), True),
        StructField("tname", StringType(), True),
        StructField("his_rank", IntegerType(), True)
    ])

    df = spark.createDataFrame(rdd, schema)
    df.createOrReplaceTempView("data")
    return spark, df


# 统计分析函数
def top_popular_up(spark, base_dir):
    result = spark.sql("""
        SELECT up, COUNT(*) AS popular_up_times 
        FROM data 
        GROUP BY up 
        ORDER BY popular_up_times DESC 
        LIMIT 10
    """)
    result.toPandas().to_csv(os.path.join(base_dir, 'top_popular_up.csv'), index=False)
    print("Top10 UP主已保存")


def top_popular_up_coin(spark, base_dir):
    result = spark.sql("""
        SELECT up, SUM(coin) AS coin 
        FROM data 
        GROUP BY up 
        ORDER BY coin DESC 
        LIMIT 10
    """)
    result.toPandas().to_csv(os.path.join(base_dir, 'top_popular_up_coin.csv'), index=False)
    print("投币最多的Top10 UP主已保存")


def top_popular_subject(spark, base_dir):
    result = spark.sql("""
        SELECT tname, COUNT(*) AS popular_subject_times 
        FROM data 
        GROUP BY tname 
        ORDER BY popular_subject_times DESC 
        LIMIT 10
    """)
    result.toPandas().to_csv(os.path.join(base_dir, 'top_popular_subject.csv'), index=False)
    print("Top10 视频分区已保存")


def top_popular_view(spark, base_dir):
    result = spark.sql("""
        SELECT title, view 
        FROM data 
        ORDER BY view DESC 
        LIMIT 10
    """)
    result.toPandas().to_csv(os.path.join(base_dir, 'video_view_data.csv'), index=False)
    print("Top10 播放量视频已保存")


def top_popular_danmaku(spark, base_dir):
    spark.sql("SELECT title, danmaku FROM data ORDER BY danmaku DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_danmaku.csv'), index=False)


def top_popular_reply(spark, base_dir):
    spark.sql("SELECT title, reply FROM data ORDER BY reply DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_reply.csv'), index=False)


def top_popular_favorite(spark, base_dir):
    spark.sql("SELECT title, favorite FROM data ORDER BY favorite DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_favorite.csv'), index=False)


def top_popular_coin(spark, base_dir):
    spark.sql("SELECT title, coin FROM data ORDER BY coin DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_coin.csv'), index=False)


def top_popular_share(spark, base_dir):
    spark.sql("SELECT title, share FROM data ORDER BY share DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_share.csv'), index=False)


def top_popular_like(spark, base_dir):
    spark.sql("SELECT title, like FROM data ORDER BY like DESC LIMIT 10").toPandas().to_csv(
        os.path.join(base_dir, 'top_popular_like.csv'), index=False)


# 词频统计
def word_count(spark, base_dir):
    def pretty_cut(sentence):
        chinese_only = ''.join(re.findall('[\u4e00-\u9fa5]', str(sentence)))
        cut_list = jieba.lcut(chinese_only, cut_all=False)
        stopwords = {'的', '了', '在', '是', '我', '有', '和', '就', '这', '也', '都', '很', '会', '上', '一', '个',
                     '中', '对', '他', '说', '以', '为', '到', '得', '能', '去', '不'}
        filtered = [word for word in cut_list if word not in stopwords and len(word) > 1]
        return filtered

    wordCount_title = spark.sql("SELECT title FROM data").rdd.flatMap(
        lambda row: pretty_cut(row['title'])
    ).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: -x[1])

    wordCountSchema = StructType([
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True)
    ])
    wordCountDF = spark.createDataFrame(wordCount_title, wordCountSchema)
    wordCountDF_filtered = wordCountDF.filter(wordCountDF["word"] != "").limit(300)

    save_path = os.path.join(base_dir, 'title_word.csv')
    wordCountDF_filtered.toPandas().to_csv(save_path, index=False)
    print("标题词频前300已保存")


# 机器学习分析（MLlib）
def ml_analysis(spark, base_dir):
    df = spark.sql("SELECT * FROM data")

    # 移除非数值特征，生成标签
    df = df.drop('up', 'time', 'title', 'desc', 'rcmd_reason', 'tname')
    df = df.withColumn('label', when(df.his_rank <= 10, 1).otherwise(0))

    # 特征向量化
    required_features = ['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like']
    assembler = VectorAssembler(inputCols=required_features, outputCol='features', handleInvalid="skip")
    transformed_data = assembler.transform(df).select('features', 'label')

    # 划分训练集和测试集
    training_data, test_data = transformed_data.randomSplit([0.8, 0.2], seed=2023)
    print(f"训练数据集总数: {training_data.count()}")
    print(f"测试数据集总数: {test_data.count()}")

    # 相关性矩阵
    cor_mat = Correlation.corr(transformed_data, "features", "spearman").head()[0]
    cor_df = pd.DataFrame(cor_mat.toArray(), columns=required_features, index=required_features)
    cor_df.to_csv(os.path.join(base_dir, 'correlation_matrix.csv'))
    print("相关性矩阵已保存")

    # 训练逻辑回归模型
    lr = LogisticRegression(labelCol='label', featuresCol='features', maxIter=15)
    model = lr.fit(training_data)
    lr_predictions = model.transform(test_data)

    # 评估
    acc_evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    acc = acc_evaluator.evaluate(lr_predictions)
    print(f"LogisticRegression Accuracy: {acc:.4f}")

    auc_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
    auc = auc_evaluator.evaluate(lr_predictions)
    print(f"LogisticRegression AUC: {auc:.4f}")

    # 保存模型
    model.write().overwrite().save(os.path.join(base_dir, "lr_model"))
    print("模型已保存")


if __name__ == '__main__':
    txt_file = 'hdfs://localhost:9000/user/hadoop/bilibili_week.txt'
    base_dir = 'static/'

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    spark, df = initialize(txt_file)

    # 执行分析
    top_popular_up(spark, base_dir)
    top_popular_subject(spark, base_dir)
    top_popular_view(spark, base_dir)
    top_popular_danmaku(spark, base_dir)
    top_popular_reply(spark, base_dir)
    top_popular_favorite(spark, base_dir)
    top_popular_coin(spark, base_dir)
    top_popular_up_coin(spark, base_dir)
    top_popular_share(spark, base_dir)
    top_popular_like(spark, base_dir)
    word_count(spark, base_dir)
    ml_analysis(spark, base_dir)

    spark.stop()
    print("所有分析任务完成！")