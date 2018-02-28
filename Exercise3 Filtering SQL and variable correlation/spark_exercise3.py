
# coding: utf-8

# In[1]:


from __future__ import print_function
from pyspark import SparkContext, SparkConf
from phone import Phone
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

import math
import pickle
import shutil
import os, errno
import time


# In[2]:


def silentremove(filedir): #helper function to clear rdd folders and their content
    try:
        shutil.rmtree(filedir)
    except FileNotFoundError:
        pass

#start_time = time.time()
#print("--- %s seconds ---" % (time.time() - start_time))

class ExerciseSet3(object):
    """
    Big Data Frameworks Exercises
    https://www.cs.helsinki.fi/courses/582740/2017/k/k/1
    """

    def __init__(self):
        """
        Initializing Spark Conf and Spark Context here
        Some Global variables can also be initialized
        """
        self.conf = (SparkConf().setMaster("local").
                     setAppName("exercise_set_2").
                     set("spark.executor.memory", "2g"))
        self.spark_context = SparkContext(conf=self.conf)

        # Have global variables here if you wish
        # self.global_variable = None



    def exercise_1(self,partitions=False):
        """
        Carat Context-factor Dataset   Download link: http://www.cs.helsinki.fi/group/carat/carat-context-factor-data.tar.gz . Download the file and unzip it. The size of the csv file 
        is about 1 Gigabyte and the number of rows in the data set is more than 11 million. The data set has the
        following attributes.

        energyRate;batteryHealth;batteryTemperature;batteryVoltage;cpuUsage;distanceTraveled;mobileDataActivity;
        mobileDataStatus;mobileNetworkType;networkType;roamingEnabled;screenBrightness;
        wifiLinkSpeed;wifiSignalStrength

        CPU load from 0 to 1 (0% to 100%)
        Distance traveled: higher than or exactly zero (binary classification)
        Screen brightness: from 0 to 255, and -1 as an automatic setting
        Wi-fi signal strength: from -100 to 0, exclusive
        Battery temperature: higher than or exactly zero

        TODO
        Consider  attribute values which are in the above mentioned range values. You will have to apply a
        filter function with a number of conditions. Then,  write a correlation function, such as pearson
        correlation (you can follow Eemil's lecture) and apply it for the following attribute pairs
        (energyRate, cpuUsage), (energyRate, screenBrightness), (energyRate, wifiLinkSpeed), and
        (energyRate, wifiSignalStrength). Use RDD to load the file and all the required transformations to
        compute the correlations.  Calculate the time taken for each correlation on attribute pairs to be completed.

        """
        print("Exercise 1")

        def corre (x,y):
            startTime = time.time()
            mx = x.sum()/x.count()
            my = y.sum()/y.count()
            both = x.zip(y)
            up = both.map(lambda pair: (pair[0]-mx)*(pair[1]-my)).sum()
            lowx = both.map(lambda pair: math.pow((pair[0]-mx),2)).sum()
            lowy = both.map(lambda pair: math.pow((pair[1]-my),2)).sum()
            r = up/(math.sqrt(lowx)*math.sqrt(lowy))
            elapsedTime = time.time() - startTime
            print('r is '+ str(r) + ' time is '+ str(elapsedTime))
            return r

        sc = self.spark_context #for convenience abbreviate spark context variable
        data = sc.textFile("/tmp/carat-context-factors-percom.csv")
        dataclean = data.map(lambda line: line.split(";"))
        datamap = dataclean.map(lambda ph: Phone(float(ph[0]),ph[1],float(ph[2]),float(ph[3]),ph[4],ph[5],ph[6],ph[7],ph[8],ph[9],float(ph[10]),
                                                 float(ph[11]),float(ph[12]),float(ph[13])))
        data_filters = datamap.filter(lambda x: ((float(x.cpuUsage) <= 1) & (float(x.cpuUsage) >= 0) & (float(x.distanceTraveled) >= 0) & (((float(x.screenBrightness) >= 0) & (float(x.screenBrightness) <= 255)) | (float(x.screenBrightness) == -1)) & (float(x.wifiSignalStrength) > -100)  & (float(x.wifiSignalStrength) < 0) & (float(x.batteryTemperature) >= 0)))

        energy_data = data_filters.map(lambda a: float(a.energyRate))
        cpu_data = data_filters.map(lambda a: float(a.cpuUsage))
        screen_data = data_filters.map(lambda a: float(a.screenBrightness))
        wilink_data = data_filters.map(lambda a: float(a.wifiLinkSpeed))
        wisig_data = data_filters.map(lambda a: float(a.wifiSignalStrength))

        r1 = corre(energy_data,cpu_data)
        r2 = corre(energy_data,screen_data)
        r3 = corre(energy_data,wilink_data)
        r4 = corre(energy_data,wisig_data)

        print('correlation energy and cpu: ' + str(r1))
        print('correlation energy and screen: ' + str(r2))
        print('correlation energy and wifi link: ' + str(r3))
        print('correlation energy and wifi signal: ' + str(r4))

        print('done')
        return None

    def exercise_2(self):
        """
        a) Load the data 'carat-context-factors-percom.csv' into a data frame object called caratDF. What's
        the schema of the data frame caratDF.

        Hint:- Load the data using the function csv. Check if the data you loaded has 14 columns. If it has
        only one column use option  sep=";"

        b) Add the following column names to caratDF.
        "energyRate","batteryHealth","batteryTemperature","batteryVoltage","cpuUsage,"distanceTraveled",
        "mobileDataActivity","mobileDataStatus","mobileNetworkType","networkType","roamingEnabled",
        "screenBrightness","wifiLinkSpeed","wifiSignalStrength"

        Hint: Pass these column names as arguments to a function called 'toDF'
        The function is available in all data frames.
        Remember to store the result back into caratDF.
        Check the columns are named using show function.

        c) Count the unique elements in the following columns. "batteryTemperature", "batteryVoltage".
        Also count the outlier data size in the above columns. Use the default value information below:

        Note:
        Battery voltage: from(>=) 2.0 to (<=) 4.35; outside this range are the outliers
        Battery temperature: from(>=) 0.0 to (<=) 50.0; outside this range are the outliers
        """

        print("Exercise 2")
        def correDF (df,x,y): #helper function to calculte correlation of columns within a dataframe
            startTime = time.time()
            r = df.corr(x,y)
            elapsedTime = time.time() - startTime
            print('r is '+ str(r) + ' time is '+ str(elapsedTime))
            return r

        sc = self.spark_context #for convenience abbreviate spark context variable
        sqlContext = SQLContext(sc) #SQL context to create dataframes and be able to perform SQL operations

        #load data as dataframes
        caratDF = None
        if os.path.exists("./caratDF"): #check if DF already exists
            caratDF = sqlContext.read.load("caratDF")
            #print("already exists")
        else:
            data = sqlContext.read.csv('/tmp/carat-context-factors-percom.csv',sep=";")
            caratDF = data.toDF("energyRate","batteryHealth","batteryTemperature","batteryVoltage","cpuUsage","distanceTraveled",
                         "mobileDataActivity","mobileDataStatus","mobileNetworkType","networkType","roamingEnabled",
                         "screenBrightness","wifiLinkSpeed","wifiSignalStrength")
            caratDF.write.save("caratDF")

        caratDF.show()

        #cast columns to doubletype
        caratDF2 = (caratDF
                    .withColumn("batteryTemperature", caratDF["batteryTemperature"].cast(DoubleType()))
                    .withColumn("batteryVoltage", caratDF["batteryVoltage"].cast(DoubleType()))
                    .withColumn("energyRate", caratDF["energyRate"].cast(DoubleType()))
                    .withColumn("cpuUsage", caratDF["cpuUsage"].cast(DoubleType()))
                    .withColumn("screenBrightness", caratDF["screenBrightness"].cast(DoubleType()))
                    .withColumn("wifiSignalStrength", caratDF["wifiSignalStrength"].cast(IntegerType()))
                    .withColumn("wifiLinkSpeed", caratDF["wifiLinkSpeed"].cast(IntegerType()))
                   )

        caratDF2.registerTempTable("rbtable") #save as temporary table for SQL queries

        #uniqueDF = sqlContext.sql("""SELECT COUNT(DISTINCT batteryTemperature,batteryVoltage) FROM rbtable""")
        uniqueDF = sqlContext.sql("""SELECT DISTINCT batteryTemperature,batteryVoltage FROM rbtable""")

        #outlierDF = sqlContext.sql("""SELECT COUNT(batteryTemperature,batteryVoltage) FROM rbtable
        #                    WHERE batteryVoltage < 2.0 OR batteryVoltage >4.35 OR batteryTemperature < 0.0
        #                    OR batteryTemperature > 50.0""")
        outlierDF = sqlContext.sql("""SELECT batteryTemperature,batteryVoltage FROM rbtable
                                    WHERE batteryVoltage < 2.0 OR batteryVoltage >4.35 OR batteryTemperature < 0.0
                                    OR batteryTemperature > 50.0""")

        #uniqueDF.show()
        #outlierDF.show()
        print('unique batteryTemperature,batteryVoltage:')
        print(uniqueDF.count())
        print('outliers:')
        print(outlierDF.count())
        #print(caratDF2.schema)

        carat = sqlContext.sql("""SELECT energyRate,cpuUsage,screenBrightness,wifiSignalStrength,wifiLinkSpeed FROM rbtable
                                WHERE cpuUsage >= 0.0 AND cpuUsage <=1.0 AND screenBrightness >= -1
                                AND screenBrightness <= 255 AND wifiSignalStrength >= -100
                                AND wifiSignalStrength < 0""")
        #carat.write.save("carat")


        r1 = correDF(carat,"energyRate","cpuUsage")
        r2 = correDF(carat,"energyRate","screenBrightness")
        r3 = correDF(carat,"energyRate","wifiSignalStrength")
        r4 = correDF(carat,"energyRate","wifiLinkSpeed")

        print('correlation energy and cpu: ' + str(r1))
        print('correlation energy and screen: ' + str(r2))
        print('correlation energy and wifi link: ' + str(r3))
        print('correlation energy and wifi signal: ' + str(r4))

        print('done')
        return None




if __name__ == "__main__":
    EXERCISESET3 = ExerciseSet3()
    EXERCISESET3.exercise_1()
    EXERCISESET3.exercise_2()
    EXERCISESET3.spark_context.stop()
