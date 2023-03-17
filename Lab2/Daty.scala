// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------

// MAGIC %python
// MAGIC import pyspark.sql.functions as fun
// MAGIC cols = ["timestamp","unix", "Date"]
// MAGIC dane = [["2015-03-22T14:13:34", 1646641525,"May, 2021"],
// MAGIC         ["2015-03-22T15:03:18", 1646641557555,"Mar, 2021"],
// MAGIC         ["2015-03-22T14:38:39", 1646641578622,"Jan, 2021"]]
// MAGIC dfp = spark.createDataFrame(dane, cols) \
// MAGIC     .withColumn("current_date", fun.current_date()) \
// MAGIC     .withColumn("current_timestamp", fun.current_timestamp()) \
// MAGIC     .withColumn("current_date_string", fun.current_date().cast('string'))
// MAGIC 
// MAGIC mod_dfp = dfp.withColumn("date_format", fun.date_format('current_date', 'dd/MM/yyyy')) \
// MAGIC             .withColumn("from_unixtime", fun.from_unixtime('unix')) \
// MAGIC             .withColumn("to_date", fun.to_date('current_date_string')) \
// MAGIC             .withColumn("to_timestamp", fun.to_timestamp('current_date_string')) \
// MAGIC             .withColumn("from_utc_timestamp", fun.from_utc_timestamp('timestamp', 'Asia/Tokyo')) \
// MAGIC             .withColumn("to_utc_timestamp", fun.to_utc_timestamp('from_utc_timestamp', 'Asia/Tokyo'))
// MAGIC display(mod_dfp)

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

// MAGIC %python
// MAGIC mod_dfp = mod_dfp.withColumn("unix_timestamp", fun.unix_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
// MAGIC display(mod_dfp)
// MAGIC mod_dfp.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------

// MAGIC %python
// MAGIC zmianaFormatu = mod_dfp.select('timestamp','unix_timestamp') \
// MAGIC                 .withColumn('unix_timestamp_formatted', mod_dfp['unix_timestamp'].cast('timestamp'))
// MAGIC display(zmianaFormatu)
// MAGIC zmianaFormatu.printSchema()

// COMMAND ----------

// MAGIC %python
// MAGIC #Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)
// MAGIC newCols = zmianaFormatu.select("timestamp").withColumn('year', fun.year('timestamp')) \
// MAGIC             .withColumn('month', fun.month('timestamp')) \
// MAGIC             .withColumn('dayofyear', fun.dayofyear('timestamp'))
// MAGIC display(newCols)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

//cmd3
//date_format
yearDate = 
display(yearDate)

// COMMAND ----------

//cmd3
//to_date()
val toDate = 
display(toDate)

// COMMAND ----------

//cmd3
//from_unixtime()
val fromUnix = 
display(fromUnix)

// COMMAND ----------

//cmd3
//to_timestamp()
val toTimestamp = 
display(toTimestamp)


// COMMAND ----------

//cmd3
//to_utc_timestamp()
val toUtcTimestamp = 
display(toUtcTimestamp)



// COMMAND ----------

//cmd3
//from_utc_timestamp()
val fromUtcTimestamp = 
display(fromUtcTimestamp)