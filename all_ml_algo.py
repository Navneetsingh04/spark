# ======================================================
#              ALL ML ALGORITHMS IN ONE FILE
#               PySpark MLlib – Complete Guide
# ======================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import PCA


# ------------------------------------------------------
# START SPARK SESSION
# ------------------------------------------------------
spark = SparkSession.builder.appName("AllAlgorithms").getOrCreate()


# ======================================================
# 1. LINEAR REGRESSION – California Housing Dataset
# ======================================================

def linear_regression_model():

    print("\n====== LINEAR REGRESSION ======\n")

    data = spark.read.csv("datasets/california_housing.csv", header=True, inferSchema=True)
    feature_cols = [c for c in data.columns if c != "median_house_value"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(data)

    train, test = df.randomSplit([0.7, 0.3], seed=10)

    lr = LinearRegression(featuresCol="features", labelCol="median_house_value")
    model = lr.fit(train)

    pred = model.transform(test)
    pred.select("prediction", "median_house_value").show(5)

    print("RMSE:", model.summary.rootMeanSquaredError)


linear_regression_model()



# ======================================================
# 2. LOGISTIC REGRESSION – Titanic Dataset
# ======================================================

def logistic_regression_model():

    print("\n====== LOGISTIC REGRESSION ======\n")

    data = spark.read.csv("datasets/titanic.csv", header=True, inferSchema=True)
    data = data.select("Pclass", "Age", "Fare", "Sex", "Survived")

    # Convert categorical "Sex" to numeric
    data = data.withColumn("Sex", when(data.Sex == "male", 1).otherwise(0))

    assembler = VectorAssembler(inputCols=["Pclass", "Age", "Fare", "Sex"],
                                outputCol="features")
    df = assembler.transform(data).withColumnRenamed("Survived", "label")

    train, test = df.randomSplit([0.7, 0.3], seed=42)

    logr = LogisticRegression(featuresCol="features", labelCol="label")
    model = logr.fit(train)

    pred = model.transform(test)
    pred.select("probability", "prediction", "label").show(5)


logistic_regression_model()



# ======================================================
# 3. DECISION TREE CLASSIFIER – Bank Dataset
# ======================================================

def decision_tree_model():

    print("\n====== DECISION TREE CLASSIFIER ======\n")

    data = spark.read.csv("datasets/bank.csv", header=True, inferSchema=True)
    data = data.select("age", "balance", "duration", "campaign", "y")

    # Convert target
    data = data.withColumn("label", when(data.y == "yes", 1).otherwise(0))

    assembler = VectorAssembler(
        inputCols=["age", "balance", "duration", "campaign"],
        outputCol="features")
    df = assembler.transform(data)

    train, test = df.randomSplit([0.7, 0.3], seed=20)

    dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")
    model = dt.fit(train)

    pred = model.transform(test)
    pred.select("prediction", "label").show(5)


decision_tree_model()



# ======================================================
# 4. RANDOM FOREST – Heart Disease Dataset
# ======================================================

def random_forest_model():

    print("\n====== RANDOM FOREST CLASSIFIER ======\n")

    data = spark.read.csv("datasets/heart.csv", header=True, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=[c for c in data.columns if c != "target"],
        outputCol="features")
    df = assembler.transform(data).withColumnRenamed("target", "label")

    train, test = df.randomSplit([0.7, 0.3], seed=33)

    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100)
    model = rf.fit(train)

    pred = model.transform(test)
    pred.select("prediction", "label").show(5)


random_forest_model()



# ======================================================
# 5. GRADIENT BOOSTED TREES – Breast Cancer Dataset
# ======================================================

def gbt_model():

    print("\n====== GRADIENT BOOSTED TREES ======\n")

    data = spark.read.csv("datasets/breast_cancer.csv", header=True, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=[c for c in data.columns if c != "diagnosis"],
        outputCol="features")
    df = assembler.transform(data)
    df = df.withColumn("label", when(df.diagnosis == "M", 1).otherwise(0))

    train, test = df.randomSplit([0.7, 0.3], seed=99)

    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=50)
    model = gbt.fit(train)

    pred = model.transform(test)
    pred.select("prediction", "label").show(5)


gbt_model()



# ======================================================
# 6. K-MEANS CLUSTERING – Mall Customers Dataset
# ======================================================

def kmeans_model():

    print("\n====== K-MEANS CLUSTERING ======\n")

    data = spark.read.csv("datasets/mall_customers.csv", header=True, inferSchema=True)
    data = data.select("Age", "Annual_Income", "Spending_Score")

    assembler = VectorAssembler(
        inputCols=["Age", "Annual_Income", "Spending_Score"],
        outputCol="features")
    df = assembler.transform(data)

    kmeans = KMeans(featuresCol="features", k=4, seed=1)
    model = kmeans.fit(df)

    clusters = model.transform(df)
    clusters.select("Age", "Annual_Income", "Spending_Score", "prediction").show(5)


kmeans_model()



# ======================================================
# 7. PCA – Dimensionality Reduction
# ======================================================

def pca_model():

    print("\n====== PRINCIPAL COMPONENT ANALYSIS (PCA) ======\n")

    data = spark.read.csv("datasets/faces.csv", header=True, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=data.columns,
        outputCol="features")
    df = assembler.transform(data)

    pca = PCA(k=10, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)

    result = model.transform(df)
    result.select("pcaFeatures").show(5)


pca_model()



# ======================================================
# STOP SPARK
# ======================================================
spark.stop()

