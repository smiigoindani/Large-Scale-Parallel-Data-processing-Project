from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
import pandas as pd
import sys
import numpy as np
from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score


conf = SparkConf().setAppName("Ensemble RF").master("local[4]")

spark = SparkContext(conf = conf)
sc = SparkSession.builder.config(conf=conf).getOrCreate()


path = sys.argv[1]
cols = cols = ["CNT_CHILDREN",
 "AMT_INCOME_TOTAL",
 "AMT_CREDIT",
 "AMT_ANNUITY",
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
 "HOUR_APPR_PROCESS_START",
 "REG_REGION_NOT_LIVE_REGION",
 "REG_REGION_NOT_WORK_REGION",
 "LIVE_REGION_NOT_WORK_REGION",
 "REG_CITY_NOT_LIVE_CITY",
 "REG_CITY_NOT_WORK_CITY",
 "LIVE_CITY_NOT_WORK_CITY",
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
 "NAME_TYPE_SUITE_Children",
 "NAME_TYPE_SUITE_Family",
 "NAME_TYPE_SUITE_Group of people",
 "NAME_TYPE_SUITE_Other_A",
 "NAME_TYPE_SUITE_Other_B",
 "NAME_TYPE_SUITE_Spouse, partner",
 "NAME_TYPE_SUITE_Unaccompanied",
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
 "WEEKDAY_APPR_PROCESS_START_SATURDAY",
 "WEEKDAY_APPR_PROCESS_START_SUNDAY",
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
 "ORGANIZATION_TYPE_XNA"]


def train_model(df):
    df_sample = df.sample(n=200000,replace= True)
    X = df_sample[cols]
    y = df_sample['TARGET']
    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
    imp = imp.fit(X_train)
    X_train = imp.transform(X_train)
    X_test = imp.transform(X_test)
    return X_train,y_train,X_test,y_test


df = pd.read_csv(path)
pd.set_option('max_colwidth', 800)
df.head()


## Models for in-memory processing
clf = tree.DecisionTreeClassifier()
clf1 = tree.DecisionTreeClassifier(max_depth =10,max_features = 100,min_samples_leaf=5)
clf2 = tree.DecisionTreeClassifier(max_depth =15,min_impurity_decrease = 0.5,max_features = 120)
clf3 = tree.DecisionTreeClassifier(max_depth =20)
ds_tree_models = [clf,clf1,clf2,clf3]


## RDD for the Decision tree models
rdd = spark.parallelize(ds_tree_models,4)
rddCollect = rdd.collect()
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))

## Random sampling of the data and dividing into train test datasets
rdd2 = rdd.map(lambda x: (x,train_model(df)))

## Fitting the training dataset into the model
rdd3 = rdd2.map(lambda x: (x[0].fit(x[1][0],x[1][1]),x[1][2],x[1][3]))

## Predicting the test data
rdd4 = rdd3.map(lambda x: (x[0].predict(x[1]),x[2]))

## Checking the accuracy for each model
accuracy_list = []
rdd5 = rdd4.map(lambda x:accuracy_score(x[1],x[0]))
for element in rdd5.collect():
    accuracy_list.append(element)


print("Final model accuracy ": pd.Series(accuracy_list).mean()*100)


