# Databricks notebook source
#Zadanie 2. 

#Wybierz jeden z plików csv z poprzednich ćwiczeń (Mini Kurs) i stwórz ręcznie schemat danych. Przykład na wykładzie. Stwórz DataFrame (DataFrameReader) wczytując plik z użyciem schematu. 
def file_exists(path):
  try:
    dbutils.fs.ls(path)
    return True 
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise
	  
	  
actorsUrl = "https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"
filePath = "/FileStore/tables/Files/"
dbutils.fs.mkdirs(filePath)
actorsFile = "actors.csv"
tmp = "file:/tmp/"
dbfsdestination = "dbfs:/FileStore/tables/Files/"

#ACTORS
import urllib.request

if (file_exists(filePath + actorsFile) == False):
  urllib.request.urlretrieve(actorsUrl,"/tmp/" + actorsFile)
  dbutils.fs.mv(tmp + actorsFile,dbfsdestination + actorsFile)
  
display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/"))
filePath = "dbfs:/FileStore/tables/Files/actors.csv"
actorsDf = spark.read.format("csv") \
            .option("header","true") \
            .option("inferSchema","true") \
            .load(filePath)
filePath2 = "dbfs:/FileStore/tables/Files/ratings.csv"
ratingsDf = spark.read.format("csv") \
            .option("header","true") \
            .option("inferSchema","true") \
            .load(filePath2)
display(actorsDf)

# COMMAND ----------

actorsDf.printSchema()
from pyspark.sql.types import *

my_schema = StructType([
    StructField('imdb_title_id', StringType(), True),
    StructField('ordering', IntegerType(), True),
    StructField('imdb_name_id', StringType(), True),
    StructField('category', StringType(), True),
    StructField('job', StringType(), True),
    StructField('characters', StringType(), True)
])

actors_from_schema = spark.read.format("csv") \
    .option("header", "true") \
    .schema(my_schema) \
    .load(filePath)

display(actors_from_schema)

# COMMAND ----------

# Zadanie 3 

# Celem tego zadanie jest stworzenie pliku json. Możesz to zrobić ręcznie do czego zachęcam, to pozwoli lepiej zrozumieć plik lub jakimś skryptem. Wykorzystaj jeden z pików z poprzedniego zadania wystarczy kilka rzędów danych z pliku csv i stworzenie/konwersję do json.  

# Stwórz schemat danych do tego pliku, tak jak w poprzednim zadaniu. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/ Zadanie 3 

actors_to_json = actors_from_schema.limit(10)
display(actors_to_json)
#actors_to_json.write.format('json').save('dbfs:/FileStore/tables/Files/act.json')
display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/"))
dbutils.fs.head("dbfs:/FileStore/tables/Files/act.json/part-00000-tid-68003141304350331-7acd3ad4-07d0-45e7-af20-f263948c2a29-468-1-c000.json")

#format już jest stworzony, więc nie tworzę
actors_from_json = spark.read.json('dbfs:/FileStore/tables/Files/act.json')
display(actors_from_json)

# COMMAND ----------

# Zadanie 4 

# Użycie Read Modes.  

# Wykorzystaj posiadane pliki bądź użyj nowe i użyj wszystkich typów read modes, zapisz co się dzieje. Poprawny plik nie wywoła żadnych efektów, więc popsuj dane tak aby każda z read modes zadziałała. 
ratings_save = ratingsDf.limit(20).select('imdb_title_id','weighted_average_vote','total_votes','mean_vote')
ratings_save.printSchema()
#ratings_save.write.format('csv').save('dbfs:/FileStore/tables/Files/mod_ratings.csv')
display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/"))

#broken, bo zmieniłem w kolumnie weighted_average_vote double na inta
broken_schema = StructType([
    StructField('imdb_title_id', StringType(), True),
    StructField('weighted_average_vote', IntegerType(), True),
    StructField('total_votes', IntegerType(), True),
    StructField('mean_vote', DoubleType(), True),
])

good_schema = StructType([
    StructField('imdb_title_id', StringType(), True),
    StructField('weighted_average_vote', DoubleType(), True),
    StructField('total_votes', IntegerType(), True),
    StructField('mean_vote', DoubleType(), True),
])

#1. Mode permissive - kolumna weighted_average_vote została wpisana jako nulle, bo były double
mode_perm = spark.read.format('csv').schema(broken_schema).option('mode', 'PERMISSIVE').load('dbfs:/FileStore/tables/Files/mod_ratings.csv')
display(mode_perm)

#2. mode dropmalformed - wczytało się bez błędów, ale mam pusty data frame - bo usunęło błędne rekordy czyli wszystkie
mode_drop = spark.read.format('csv').schema(broken_schema).option('mode', 'DROPMALFORMED').load('dbfs:/FileStore/tables/Files/mod_ratings.csv')
display(mode_drop)

#3. mode failfast - wyskoczył fail, bo się nie zgadzało
# mode_fail = spark.read.format('csv').schema(broken_schema).option('mode', 'FAILFAST').load('dbfs:/FileStore/tables/Files/mod_ratings.csv')
# display(mode_fail)

#4. badRecordPath - no tu to nie wiem co się stało
mode_bad = spark.read.format('csv').schema(broken_schema).option('badRecordsPath', 'dbfs:/FileStore/tables/Files/mod_ratings_error.csv').load('dbfs:/FileStore/tables/Files/mod_ratings.csv')
display(mode_bad)
bad_records = spark.read.format('csv').schema(good_schema).load('dbfs:/FileStore/tables/Files/mod_ratings_error.csv')
display(bad_records)


# COMMAND ----------

# Zadanie 5 

# Użycie DataFrameWriter. 

# Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego spark.read (DataFrameReader).  
ratings_save.write.format('json').save('dbfs:/FileStore/tables/Files/check_ratings.json')
ratings_save.write.format('parquet').save('dbfs:/FileStore/tables/Files/check_ratings.parquet')

ratings_parq = spark.read.format('parquet').load('dbfs:/FileStore/tables/Files/check_ratings.parquet')
display(ratings_parq)

ratings_json = spark.read.format('json').load('dbfs:/FileStore/tables/Files/check_ratings.json')
display(ratings_json)

#elegancko się zgadzają