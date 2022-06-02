package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer,VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors


object RandomForestBagging {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Random Forest")
    val sc = new SparkContext(conf)
     val ss = SparkSession.builder().appName("Random Forest").getOrCreate()

// Load and parse the data file.
val data = ss.read.format("csv").option("inferSchema","true").option("header","true").load(args(0))
data.printSchema()


val cols = Array(
"CNT_CHILDREN",
 "AMT_INCOME_TOTAL",
 "AMT_CREDIT",
 "AMT_GOODS_PRICE",
 "REGION_POPULATION_RELATIVE",
 "DAYS_BIRTH",
 "DAYS_EMPLOYED",
 "DAYS_REGISTRATION",
 "DAYS_ID_PUBLISH",
 "OWN_CAR_AGE",
 "CNT_FAM_MEMBERS",
 "REGION_RATING_CLIENT",
 "REGION_RATING_CLIENT_W_CITY",
 "EXT_SOURCE_1",
 "EXT_SOURCE_2",
 "EXT_SOURCE_3",
 "APARTMENTS_AVG",
 "OBS_30_CNT_SOCIAL_CIRCLE",
 "DEF_30_CNT_SOCIAL_CIRCLE",
 "OBS_60_CNT_SOCIAL_CIRCLE",
 "DEF_60_CNT_SOCIAL_CIRCLE",
 "AMT_REQ_CREDIT_BUREAU_HOUR",
 "AMT_REQ_CREDIT_BUREAU_DAY",
 "AMT_REQ_CREDIT_BUREAU_WEEK",
 "AMT_REQ_CREDIT_BUREAU_MON",
 "AMT_REQ_CREDIT_BUREAU_QRT",
 "AMT_REQ_CREDIT_BUREAU_YEAR",
 "CODE_GENDER_F",
 "CODE_GENDER_M",
 "CODE_GENDER_XNA",
 "FLAG_OWN_CAR_N",
 "FLAG_OWN_CAR_Y",
 "FLAG_OWN_REALTY_N",
 "FLAG_OWN_REALTY_Y",
 "NAME_CONTRACT_TYPE_Cash loans",
 "NAME_CONTRACT_TYPE_Revolving loans",
 "NAME_INCOME_TYPE_Businessman",
 "NAME_INCOME_TYPE_Commercial associate",
 "NAME_INCOME_TYPE_Maternity leave",
 "NAME_INCOME_TYPE_Pensioner",
 "NAME_INCOME_TYPE_State servant",
 "NAME_INCOME_TYPE_Student",
 "NAME_INCOME_TYPE_Unemployed",
 "NAME_INCOME_TYPE_Working",
 "NAME_EDUCATION_TYPE_Academic degree",
 "NAME_EDUCATION_TYPE_Higher education",
 "NAME_EDUCATION_TYPE_Incomplete higher",
 "NAME_EDUCATION_TYPE_Lower secondary",
 "NAME_EDUCATION_TYPE_Secondary / secondary special",
 "NAME_FAMILY_STATUS_Civil marriage",
 "NAME_FAMILY_STATUS_Married",
 "NAME_FAMILY_STATUS_Separated",
 "NAME_FAMILY_STATUS_Single / not married",
 "NAME_FAMILY_STATUS_Unknown",
 "NAME_FAMILY_STATUS_Widow",
 "NAME_HOUSING_TYPE_Co-op apartment",
 "NAME_HOUSING_TYPE_House / apartment",
 "NAME_HOUSING_TYPE_Municipal apartment",
 "NAME_HOUSING_TYPE_Office apartment",
 "NAME_HOUSING_TYPE_Rented apartment",
 "NAME_HOUSING_TYPE_With parents",
 "OCCUPATION_TYPE_Accountants",
 "OCCUPATION_TYPE_Cleaning staff",
 "OCCUPATION_TYPE_Cooking staff",
 "OCCUPATION_TYPE_Core staff",
 "OCCUPATION_TYPE_Drivers",
 "OCCUPATION_TYPE_HR staff",
 "OCCUPATION_TYPE_High skill tech staff",
 "OCCUPATION_TYPE_IT staff",
 "OCCUPATION_TYPE_Laborers",
 "OCCUPATION_TYPE_Low-skill Laborers",
 "OCCUPATION_TYPE_Managers",
 "OCCUPATION_TYPE_Medicine staff",
 "OCCUPATION_TYPE_Other",
 "OCCUPATION_TYPE_Private service staff",
 "OCCUPATION_TYPE_Realty agents",
 "OCCUPATION_TYPE_Sales staff",
 "OCCUPATION_TYPE_Secretaries",
 "OCCUPATION_TYPE_Security staff",
 "OCCUPATION_TYPE_Waiters/barmen staff",
 "WEEKDAY_APPR_PROCESS_START_FRIDAY",
 "WEEKDAY_APPR_PROCESS_START_MONDAY",
 "WEEKDAY_APPR_PROCESS_START_THURSDAY",
 "WEEKDAY_APPR_PROCESS_START_TUESDAY",
 "WEEKDAY_APPR_PROCESS_START_WEDNESDAY",
 "ORGANIZATION_TYPE_Advertising",
 "ORGANIZATION_TYPE_Agriculture",
 "ORGANIZATION_TYPE_Bank",
 "ORGANIZATION_TYPE_Business Entity Type 1",
 "ORGANIZATION_TYPE_Business Entity Type 2",
 "ORGANIZATION_TYPE_Business Entity Type 3",
 "ORGANIZATION_TYPE_Cleaning",
 "ORGANIZATION_TYPE_Construction",
 "ORGANIZATION_TYPE_Culture",
 "ORGANIZATION_TYPE_Electricity",
 "ORGANIZATION_TYPE_Emergency",
 "ORGANIZATION_TYPE_Government",
 "ORGANIZATION_TYPE_Hotel",
 "ORGANIZATION_TYPE_Housing",
 "ORGANIZATION_TYPE_Industry: type 1",
 "ORGANIZATION_TYPE_Industry: type 10",
 "ORGANIZATION_TYPE_Industry: type 11",
 "ORGANIZATION_TYPE_Industry: type 12",
 "ORGANIZATION_TYPE_Industry: type 13",
 "ORGANIZATION_TYPE_Industry: type 2",
 "ORGANIZATION_TYPE_Industry: type 3",
 "ORGANIZATION_TYPE_Industry: type 4",
 "ORGANIZATION_TYPE_Industry: type 5",
 "ORGANIZATION_TYPE_Industry: type 6",
 "ORGANIZATION_TYPE_Industry: type 7",
 "ORGANIZATION_TYPE_Industry: type 8",
 "ORGANIZATION_TYPE_Industry: type 9",
 "ORGANIZATION_TYPE_Insurance",
 "ORGANIZATION_TYPE_Kindergarten",
 "ORGANIZATION_TYPE_Legal Services",
 "ORGANIZATION_TYPE_Medicine",
 "ORGANIZATION_TYPE_Military",
 "ORGANIZATION_TYPE_Mobile",
 "ORGANIZATION_TYPE_Other",
 "ORGANIZATION_TYPE_Police",
 "ORGANIZATION_TYPE_Postal",
 "ORGANIZATION_TYPE_Realtor",
 "ORGANIZATION_TYPE_Religion",
 "ORGANIZATION_TYPE_Restaurant",
 "ORGANIZATION_TYPE_School",
 "ORGANIZATION_TYPE_Security",
 "ORGANIZATION_TYPE_Security Ministries",
 "ORGANIZATION_TYPE_Self-employed",
 "ORGANIZATION_TYPE_Services",
 "ORGANIZATION_TYPE_Telecom",
 "ORGANIZATION_TYPE_Trade: type 1",
 "ORGANIZATION_TYPE_Trade: type 2",
 "ORGANIZATION_TYPE_Trade: type 3",
 "ORGANIZATION_TYPE_Trade: type 4",
 "ORGANIZATION_TYPE_Trade: type 5",
 "ORGANIZATION_TYPE_Trade: type 6",
 "ORGANIZATION_TYPE_Trade: type 7",
 "ORGANIZATION_TYPE_Transport: type 1",
 "ORGANIZATION_TYPE_Transport: type 2",
 "ORGANIZATION_TYPE_Transport: type 3",
 "ORGANIZATION_TYPE_Transport: type 4",
 "ORGANIZATION_TYPE_University",
 "DAYS_CREDIT",
 "CREDIT_DAY_OVERDUE",
 "DAYS_CREDIT_ENDDATE",
 "DAYS_ENDDATE_FACT",
 "CNT_CREDIT_PROLONG",
 "AMT_CREDIT_SUM",
 "AMT_CREDIT_SUM_DEBT",
 "AMT_CREDIT_SUM_LIMIT",
 "AMT_CREDIT_SUM_OVERDUE",
 "DAYS_CREDIT_UPDATE",
 "AMT_ANNUITY_y",
 "MONTHS_BALANCE",
 "CREDIT_ACTIVE_Active",
 "CREDIT_ACTIVE_Bad debt",
 "CREDIT_ACTIVE_Closed",
 "CREDIT_ACTIVE_Sold",
 "CREDIT_CURRENCY_currency 1",
 "CREDIT_CURRENCY_currency 2",
 "CREDIT_CURRENCY_currency 3",
 "CREDIT_CURRENCY_currency 4",
 "CREDIT_TYPE_Another type of loan",
 "CREDIT_TYPE_Car loan",
 "CREDIT_TYPE_Cash loan (non-earmarked)",
 "CREDIT_TYPE_Consumer credit",
 "CREDIT_TYPE_Credit card",
 "CREDIT_TYPE_Loan for business development",
 "CREDIT_TYPE_Loan for purchase of shares (margin lending)",
 "CREDIT_TYPE_Loan for the purchase of equipment",
 "CREDIT_TYPE_Loan for working capital replenishment",
 "CREDIT_TYPE_Microloan",
 "CREDIT_TYPE_Mobile operator loan",
 "CREDIT_TYPE_Mortgage",
 "CREDIT_TYPE_Real estate loan",
 "CREDIT_TYPE_Unknown type of loan",
 "STATUS_0",
 "STATUS_1",
 "STATUS_2",
 "STATUS_3",
 "STATUS_4",
 "STATUS_5",
 "STATUS_C",
 "STATUS_X")

// Adding a label column to the DataFrame with the the values of target variable column
val labelIndexer = new StringIndexer()
  .setInputCol("TARGET")
  .setOutputCol("indexedLabel")
  .setHandleInvalid("skip")
  .fit(data)


// Adding a vector feature column into the DataFrame,with al the relevant columns
val assembler = new VectorAssembler()
  .setInputCols(cols)
  .setOutputCol("indexedFeatures")
  .setHandleInvalid("skip")


val seed = 5043
// Split the data into training and test sets (30% held out for testing)
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

// Train a RandomForest model.

val randomForestClassifier = new RandomForestClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("indexedFeatures")
  .setImpurity("gini")
  .setMaxDepth(10)
  .setNumTrees(25)
  .setFeatureSubsetStrategy("auto")
  .setSeed(seed)



val stages = Array(labelIndexer, assembler, randomForestClassifier)

// Build pipeline
val pipeline = new Pipeline().setStages(stages)

// Train model on the data
val model = pipeline.fit(trainingData)

// Get feature importance values
var feature_imp = model.stages(2).asInstanceOf[RandomForestClassificationModel].featureImportances.toDense


// Make predictions
val predictions = model.transform(testData)

// Evaluating the accuracy using Area under ROC as the metric
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setMetricName("areaUnderROC")

// measure the accuracy
val accuracy = evaluator.evaluate(predictions)
println("Accuracy : " + accuracy)


// Save and load model
//model.save(sc, "target/tmp/myRandomForestClassificationModel")
//val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
  }}