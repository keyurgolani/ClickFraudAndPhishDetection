// Databricks notebook source
// MAGIC %md
// MAGIC # Analyze Radios : Nova & NRJ & Virgin & Skyrock
// MAGIC Their musical history is stored there since 2005 http://www.novaplanet.com/radionova/cetaitquoicetitre/1483102800
// MAGIC 
// MAGIC By extracting each song and mixing it with [Spotify API](https://developer.spotify.com/web-api/) we can have some fun

// COMMAND ----------

case class Song(timestamp:Int, humanDate:Long, year:Int, month:Int, day:Int, hour:Int, minute: Int, artist:String, allArtists: String, title:String)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # Nova "What was this song?" page in SQL 
// MAGIC Their songs are available in the table "nova", now we can query their database in various ways thanks to SparkSQL :)
// MAGIC 
// MAGIC [An example of their "What was this song page"](http://www.novaplanet.com/radionova/cetaitquoicetitre/1483102800)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd-HH') as timestamp_parsed_timezone
// MAGIC FROM nova
// MAGIC ORDER BY timestamp 
// MAGIC LIMIT 10

// COMMAND ----------

// MAGIC %md 
// MAGIC # Data Quality Check
// MAGIC 
// MAGIC In august, some songs share the same timestamp where as they broacast on different day affecting some night's history of August 2016:
// MAGIC * August 14th : http://www.novaplanet.com/radionova/cetaitquoicetitre/1471132800
// MAGIC * August 4th : http://www.novaplanet.com/radionova/cetaitquoicetitre/1470268800
// MAGIC 
// MAGIC So we need to use a [DISTINCT on the timestamp column](http://stackoverflow.com/a/612295/3535853) on our dataset

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM nova
// MAGIC WHERE timestamp IN (
// MAGIC   SELECT timestamp
// MAGIC   FROM (
// MAGIC     SELECT COUNT(*) as same_timestamp, timestamp
// MAGIC     FROM nova
// MAGIC     GROUP BY timestamp
// MAGIC     HAVING COUNT(*) > 1
// MAGIC   )
// MAGIC )
// MAGIC ORDER BY timestamp

// COMMAND ----------

/*val nova2016 = sqlContext.sql("""
SELECT *, 'nova' as radio
FROM nova
WHERE humanDate IN (
  SELECT minHumanDate
  FROM (
    SELECT MIN(humanDate) as minHumanDate, timestamp
    FROM NOVA
    GROUP BY timestamp
  )
)
""").as[Song]
nova2016.createOrReplaceTempView("nova") //replace the table nova
nova2016.printSchema*/

// COMMAND ----------

/*val nrj2016 = sqlContext.sql("""
SELECT *, 'nrj' as radio
FROM nrj2016
WHERE humanDate IN (
  SELECT minHumanDate
  FROM (
    SELECT MIN(humanDate) as minHumanDate, timestamp
    FROM nrj2016
    GROUP BY timestamp
  )
)
""").as[Song]
nrj2016.createOrReplaceTempView("nrj2016")
nrj2016.printSchema
*/

// COMMAND ----------

/*val virgin2016 = sqlContext.sql("""
SELECT *, 'virgin' as radio
FROM virgin2016
WHERE humanDate IN (
  SELECT minHumanDate
  FROM (
    SELECT MIN(humanDate) as minHumanDate, timestamp
    FROM virgin2016
    GROUP BY timestamp
  )
)
""").as[Song]
virgin2016.createOrReplaceTempView("virgin2016")
virgin2016.printSchema*/

// COMMAND ----------

/*val skyrock2016 = sqlContext.sql("""
SELECT *, 'skyrock' as radio
FROM skyrock2016
WHERE title != '' AND artist != ''AND humanDate IN (
  SELECT minHumanDate
  FROM (
    SELECT MIN(humanDate) as minHumanDate, timestamp
    FROM skyrock2016
    GROUP BY timestamp
  )
)
""").as[Song]
skyrock2016.createOrReplaceTempView("skyrock2016")
skyrock2016.printSchema*/

// COMMAND ----------

/*val nrjnova = nrj2016.union(nova2016.union(virgin2016).union(skyrock2016))

nrjnova.createOrReplaceTempView("nrjnova")
nrjnova.cache()

nrjnova.write.parquet("nrjnovavirginskyrock.parquet")
*/
val nrjnova = spark.read.parquet("nrjnovavirginskyrock_2.parquet")
nrjnova.createOrReplaceTempView("nrjnova")
nrjnova.cache()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM nrjnova

// COMMAND ----------

// MAGIC %md
// MAGIC # Songs brainwashing ?
// MAGIC We often get sick when we listen too many times to a song on the radio, **let's see how Nova deals with brainwashing** 
// MAGIC 
// MAGIC In the the charts below, we can see Nova broadcasts a maximum of **5 times a song per day**, with a average of **1.12 per day**. In details :
// MAGIC 
// MAGIC * 0.03% :	5 times a day
// MAGIC * 0.14% :	4 times a day
// MAGIC * 0.79% :	3 times a day
// MAGIC * 8.29% :	2 times a day
// MAGIC * 90.26% :	1 time a day
// MAGIC 
// MAGIC When a song is broadcasted several times a day, you will generally listen to it again  to **7.71 hours to hear** it again.
// MAGIC 
// MAGIC 
// MAGIC Nova also broadcasted a average of **170 distinct songs per day.**
// MAGIC 
// MAGIC **Nice eclecticism, Nova !**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC FROM nrjnova
// MAGIC GROUP BY artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs_broadcasted, radio
// MAGIC FROM nrjnova
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT(artist, title)) as number_of_different_tracks, radio
// MAGIC FROM nrjnova
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_songs_broadcasted, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC FROM nrjnova
// MAGIC WHERE DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '01' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '03' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '05' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '07' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '10' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '12' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '15' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '20' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '23' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '25' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '28'
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(number_songs_broadcasted) as average_number, AVG(number_songs_broadcasted) * 3.3 / (24 * 60) * 100 as percent_of_music, radio
// MAGIC FROM (
// MAGIC   SELECT COUNT(*) as number_songs_broadcasted, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC   FROM nrjnova
// MAGIC   GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC   HAVING COUNT(*) > 0
// MAGIC   ORDER BY date
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_songs_broadcasted, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC FROM nrjnova
// MAGIC WHERE DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '01' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '05' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '10' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '15' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '20' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '25'
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs_broadcasted, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM') as date, radio
// MAGIC FROM nrjnova
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM'), radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(number_of_tracks)) as number_of_tracks, radio, hour
// MAGIC FROM (
// MAGIC   SELECT COUNT(*) as number_of_tracks, 	weekofyear( CAST(timestamp as timestamp))  as week_number, CAST(DATE_FORMAT(CAST(timestamp as timestamp),'k') as int) as hour, radio
// MAGIC   FROM nrjnova
// MAGIC   WHERE  DATE_FORMAT(CAST(timestamp as timestamp),'EEEE') = "Monday"
// MAGIC   GROUP BY 	weekofyear( CAST(timestamp as timestamp)), DATE_FORMAT(CAST(timestamp as timestamp),'k'), radio
// MAGIC   HAVING COUNT(*) > 0
// MAGIC )
// MAGIC GROUP BY hour, radio
// MAGIC ORDER BY hour 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(number_of_tracks) * 3.3) as minutes_of_music, 5 as minutes_host_speaking, 60 - 5 - ROUND(AVG(number_of_tracks) * 3.3 ) as advertising,ROUND(AVG(number_of_tracks) * 3.3 / 60 * 100) as percentage_of_music_per_hour, radio, hour as hour_of_the_day, weekday
// MAGIC FROM (
// MAGIC   SELECT COUNT(*) as number_of_tracks, 	weekofyear( CAST(timestamp as timestamp)) as week_number, CAST(DATE_FORMAT(CAST(timestamp as timestamp),'k') as int) as hour, radio, DATE_FORMAT(CAST(timestamp as timestamp),'EEEE') as weekday
// MAGIC   FROM nrjnova
// MAGIC   WHERE  DATE_FORMAT(CAST(timestamp as timestamp),'EEEE') = "Monday"
// MAGIC   GROUP BY 	weekofyear( CAST(timestamp as timestamp)), DATE_FORMAT(CAST(timestamp as timestamp),'k'), radio,DATE_FORMAT(CAST(timestamp as timestamp),'EEEE')
// MAGIC   HAVING COUNT(*) > 0
// MAGIC )
// MAGIC WHERE hour = 12
// MAGIC GROUP BY hour, radio, weekday
// MAGIC ORDER BY minutes_of_music DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd k') as date, timestamp, radio
// MAGIC FROM nrjnova
// MAGIC WHERE DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') = "2016-04-16"
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd k'), radio, timestamp
// MAGIC ORDER BY timestamp, radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd k') as date, timestamp, radio
// MAGIC FROM nrjnova
// MAGIC WHERE DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') = "2016-04-17"
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd k'), radio, timestamp
// MAGIC ORDER BY timestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs, number_of_broadcast_per_day, radio
// MAGIC FROM(
// MAGIC   SELECT COUNT(*) as number_of_broadcast_per_day, artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC   FROM nrjnova
// MAGIC   GROUP BY artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC   ORDER BY number_of_broadcast_per_day DESC
// MAGIC )
// MAGIC GROUP BY number_of_broadcast_per_day, radio
// MAGIC ORDER BY number_of_songs DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(number_of_broadcast_per_day) as average_song_broadcast_per_day, radio
// MAGIC FROM(
// MAGIC   SELECT COUNT(*) as number_of_broadcast_per_day, artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC   FROM nrjnova
// MAGIC   GROUP BY artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC   HAVING COUNT(*) >= 1
// MAGIC   ORDER BY number_of_broadcast_per_day DESC
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %md
// MAGIC # What's is the minimum duration between a song broadcasted at least twice in a day?

// COMMAND ----------

sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast_per_day, artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as dateDay, radio
  FROM nrjnova
  GROUP BY artist, title, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
  HAVING COUNT(*) > 1
""").createOrReplaceTempView("tmp_multiple_broadcast_a_day") 

val multiple_broadcast_a_day = sqlContext.sql("""
SELECT n.timestamp,n.artist, n.title, dateDay, n.humanDate, n.radio
FROM nrjnova n
JOIN tmp_multiple_broadcast_a_day m
ON m.artist = n.artist AND m.title = n.title AND m.dateDay = DATE_FORMAT(CAST(n.timestamp as timestamp),'Y-MM-dd')
ORDER BY n.artist, dateDay
""")

multiple_broadcast_a_day.createOrReplaceTempView("multiple_broadcast_a_day") 
multiple_broadcast_a_day.cache()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs, hours_between_same_song, radio, artist, title
// MAGIC FROM (
// MAGIC   SELECT MIN( ROUND(  ABS(n1.timestamp - n2.timestamp ) / (60 * 60)) ) as hours_between_same_song, n1.timestamp as ts1, n2.timestamp as ts2, n1.humanDate, n2.humanDate, n1.radio, n1.artist, n1.title
// MAGIC   FROM multiple_broadcast_a_day n1
// MAGIC   JOIN multiple_broadcast_a_day n2
// MAGIC   ON n1.artist = n2.artist AND n1.title = n2.title AND n1.dateDay = n2.dateDay AND n1.timestamp != n2.timestamp
// MAGIC   GROUP BY n1.artist, n1.title, n1.dateDay,n1.timestamp, n2.timestamp, n1.humanDate, n2.humanDate, n1.radio
// MAGIC )
// MAGIC GROUP BY hours_between_same_song, radio, artist, title
// MAGIC ORDER BY hours_between_same_song DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs, hours_between_same_song, radio, artist, title
// MAGIC FROM (
// MAGIC   SELECT MIN( ROUND(  ABS(n1.timestamp - n2.timestamp ) / (60 * 60)) ) as hours_between_same_song, n1.timestamp as ts1, n2.timestamp as ts2, n1.humanDate, n2.humanDate, n1.radio, n1.artist, n1.title
// MAGIC   FROM multiple_broadcast_a_day n1
// MAGIC   JOIN multiple_broadcast_a_day n2
// MAGIC   ON n1.artist = n2.artist AND n1.title = n2.title AND n1.dateDay = n2.dateDay AND n1.timestamp != n2.timestamp
// MAGIC   GROUP BY n1.artist, n1.title, n1.dateDay,n1.timestamp, n2.timestamp, n1.humanDate, n2.humanDate, n1.radio
// MAGIC )
// MAGIC GROUP BY hours_between_same_song, radio, artist, title
// MAGIC ORDER BY hours_between_same_song

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs, hours_between_same_song, radio, artist, title
// MAGIC FROM (
// MAGIC   SELECT MIN( ROUND(  ABS(n1.timestamp - n2.timestamp ) / (60 * 60)) ) as hours_between_same_song, n1.timestamp as ts1, n2.timestamp as ts2, n1.humanDate, n2.humanDate, n1.radio, n1.artist, n1.title
// MAGIC   FROM multiple_broadcast_a_day n1
// MAGIC   JOIN multiple_broadcast_a_day n2
// MAGIC   ON n1.artist = n2.artist AND n1.title = n2.title AND n1.dateDay = n2.dateDay AND n1.timestamp != n2.timestamp
// MAGIC   GROUP BY n1.artist, n1.title, n1.dateDay,n1.timestamp, n2.timestamp, n1.humanDate, n2.humanDate, n1.radio
// MAGIC )
// MAGIC WHERE radio = 'skyrock'
// MAGIC GROUP BY hours_between_same_song, radio, artist, title
// MAGIC ORDER BY hours_between_same_song DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(hours_between_same_song) as Average_hours_between_same_song, radio
// MAGIC FROM (
// MAGIC   SELECT MIN( ROUND(  ABS(n1.timestamp - n2.timestamp ) / (60 * 60)) ) as hours_between_same_song, n1.timestamp as ts1, n2.timestamp as ts2, n1.humanDate, n2.humanDate, n1.radio
// MAGIC   FROM multiple_broadcast_a_day n1, multiple_broadcast_a_day n2
// MAGIC   WHERE n1.artist = n2.artist AND n1.title = n2.title AND n1.dateDay = n2.dateDay AND n1.timestamp != n2.timestamp AND n1.radio = n2.radio
// MAGIC   GROUP BY n1.artist, n1.title, n1.dateDay,n1.timestamp, n2.timestamp, n1.humanDate, n2.humanDate, n1.radio
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT(artist, title)) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd') as date, radio
// MAGIC FROM nrjnova
// MAGIC WHERE DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '01' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '03' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '05' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '07' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '10' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '12' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '15' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '20' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '23' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '25' OR
// MAGIC       DATE_FORMAT(CAST(timestamp as timestamp),'dd') = '28'
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(number_of_different_tracks)) as average_different_songs_per_day, radio
// MAGIC FROM (
// MAGIC             SELECT  COUNT(DISTINCT(artist, title)) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC             FROM nrjnova
// MAGIC             GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC             HAVING COUNT(DISTINCT(artist, title)) >= 1
// MAGIC  )
// MAGIC  GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(number_of_tracks)) as average_songs_per_day, radio
// MAGIC FROM (
// MAGIC             SELECT(COUNT(artist, title)) as number_of_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC             FROM nrjnova
// MAGIC             GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd'), radio
// MAGIC             HAVING (COUNT(artist, title)) >= 1
// MAGIC  )
// MAGIC  GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(number_of_different_tracks)) as average_different_songs_per_month, monthYear, radio
// MAGIC FROM (
// MAGIC             SELECT(COUNT(DISTINCT(artist, title))) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'YMM') as monthYear, radio
// MAGIC             FROM nrjnova
// MAGIC             GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'YMM'), radio
// MAGIC  )
// MAGIC GROUP BY monthYear, radio
// MAGIC ORDER BY monthYear

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(average_different_songs_per_month), radio
// MAGIC FROM (
// MAGIC   SELECT ROUND(AVG(number_of_different_tracks)) as average_different_songs_per_month, monthYear, radio
// MAGIC   FROM (
// MAGIC               SELECT(COUNT(DISTINCT(artist, title))) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'YMM') as monthYear, radio
// MAGIC               FROM nrjnova
// MAGIC               GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'YMM'), radio
// MAGIC    )
// MAGIC   GROUP BY monthYear, radio
// MAGIC   ORDER BY monthYear
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT(COUNT(artist, title)) as number_of_different_tracks, DATE_FORMAT(CAST(timestamp as timestamp),'YMM') as monthYear, radio
// MAGIC FROM nrjnova
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'YMM'), radio
// MAGIC ORDER BY monthYear

// COMMAND ----------

// MAGIC %md
// MAGIC # Radio Nova's Favorite songs and artists

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'nova'
// MAGIC GROUP BY artist, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'nrj'
// MAGIC GROUP BY artist, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'virgin'
// MAGIC GROUP BY artist, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'skyrock'
// MAGIC GROUP BY artist, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #Top 10

// COMMAND ----------

val top10Virgin = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, radio,title
FROM nrjnova
WHERE radio = 'virgin'
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

top10Virgin.createOrReplaceTempView("top10Virgin")

val top10Nova = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist,title,  radio
FROM nrjnova
WHERE radio = 'nova'
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

top10Nova.createOrReplaceTempView("top10Nova")

val top10Skyrock = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova
WHERE radio = 'skyrock'
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

top10Skyrock.createOrReplaceTempView("top10Skyrock")

val top10NRJ = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, radio,title
FROM nrjnova
WHERE radio = 'nrj'
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

top10NRJ.createOrReplaceTempView("top10NRJ")
val top10NRJVirgin = sqlContext.sql("""
SELECT n.artist, n.radio, n.title
FROM top10NRJ n
JOIN top10Virgin v
ON n.artist = v.artist AND n.title = v.title
""")

top10NRJVirgin.createOrReplaceTempView("top10NRJVirgin")



// COMMAND ----------

    val summerTop10Virgin = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, radio, title
FROM nrjnova n1
WHERE radio = 'virgin'  AND (DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-07' OR DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-08')
GROUP BY artist, radio, title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

summerTop10Virgin.createOrReplaceTempView("summerTop10Virgin")


    val summerTop10Nova = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist,title,  radio
FROM nrjnova n1
WHERE radio = 'nova'  AND (DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-07' OR DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-08')
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

    summerTop10Nova.createOrReplaceTempView("summerTop10Nova")

    val summerTop10Skyrock = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova n1
WHERE radio = 'skyrock'  AND (DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-07' OR DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-08')
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

    summerTop10Skyrock.createOrReplaceTempView("summerTop10Skyrock")


    val summerTop10NRJ = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, radio,title
FROM nrjnova n1
WHERE radio = 'nrj'  AND (DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-07' OR DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') = '2016-08')
GROUP BY artist, radio,title
ORDER BY number_of_broadcast DESC
LIMIT 10
""")

    summerTop10NRJ.createOrReplaceTempView("summerTop10NRJ")

    val summerTop10NRJVirgin = sqlContext.sql("""
SELECT n.artist, n.radio, n.title
FROM summerTop10NRJ n
JOIN summerTop10Virgin v
ON n.artist = v.artist AND n.title = v.title
""")
summerTop10NRJVirgin.createOrReplaceTempView("summerTop10NRJVirgin")

// COMMAND ----------

top10Nova.union(top10NRJ).union(top10Virgin).createOrReplaceTempView("top10")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio, n1.title
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'nova' AND (artist,title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM top10Nova
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'skyrock' AND (artist,title) IN (
// MAGIC   SELECT artist, title
// MAGIC   FROM top10Skyrock
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio, n1.title
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'nrj' AND (artist,title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM top10NRJ
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio, n1.title
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'virgin' AND(artist, title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM top10Virgin
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE artist IN (
// MAGIC   SELECT artist
// MAGIC   FROM top10NRJVirgin
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %md
// MAGIC # Summer hits broadcasts

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'nrj' AND(artist, title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM summerTop10NRJ
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'nova' AND(artist, title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM summerTop10Nova
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'virgin' AND(artist, title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM summerTop10Virgin
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(*) as number_of_broadcast, n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM') as date, n1.radio
// MAGIC FROM nrjnova n1
// MAGIC WHERE n1.radio = 'skyrock' AND(artist, title) IN (
// MAGIC   SELECT artist,title
// MAGIC   FROM summerTop10Skyrock
// MAGIC )
// MAGIC GROUP BY n1.artist, n1.title, DATE_FORMAT(CAST(n1.timestamp as timestamp),'Y-MM'), n1.radio
// MAGIC ORDER BY date, number_of_broadcast

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, title, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'nova' 
// MAGIC GROUP BY artist, title, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, title, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'nrj' 
// MAGIC GROUP BY artist, title, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, title, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'virgin' 
// MAGIC GROUP BY artist, title, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_broadcast, artist, title, radio
// MAGIC FROM nrjnova
// MAGIC WHERE radio = 'skyrock' 
// MAGIC GROUP BY artist, title, radio
// MAGIC ORDER BY number_of_broadcast DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT COUNT(DISTINCT(n3.artist, n3.title)) as number_of_diff_songs, n3.radio
// MAGIC FROM nrjnova n3
// MAGIC GROUP BY n3.radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT n1.artist, n1.title) as number_of_similar_songs, CONCAT(n1.radio, "-", n2.radio) as radios, n1.radio as radio_1, ROUND(COUNT(DISTINCT n1.artist, n1.title) / number_of_song_radio_1 * 100) as percent_radio_1, number_of_song_radio_1, n2.radio as radio_2,ROUND(COUNT(DISTINCT n1.artist, n1.title) / number_of_song_radio_2 * 100) as percent_radio_2, number_of_song_radio_2
// MAGIC FROM nrjnova n1
// MAGIC JOIN nrjnova n2
// MAGIC ON n1.radio < n2.radio AND LOWER(n1.artist) =  LOWER(n2.artist) AND  LOWER(n1.title) =  LOWER(n2.title)-- thanks http://stackoverflow.com/a/13041833/3535853 for the "<" trick
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_1, radio
// MAGIC       FROM nrjnova n3
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal1
// MAGIC ON n1.radio = subTotal1.radio
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_2, radio
// MAGIC       FROM nrjnova n4
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal2
// MAGIC ON n2.radio = subTotal2.radio 
// MAGIC GROUP BY n1.radio, n2.radio, number_of_song_radio_1, number_of_song_radio_2
// MAGIC ORDER BY number_of_similar_songs DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT n1.artist, n1.title) as number_of_similar_songs, CONCAT(n1.radio, "-", n2.radio) as radios, n1.radio as radio_1, ROUND(COUNT(DISTINCT n1.artist, n1.title) / number_of_song_radio_1 * 100) as percent_radio_1, number_of_song_radio_1, n2.radio as radio_2,ROUND(COUNT(DISTINCT n1.artist, n1.title) / number_of_song_radio_2 * 100) as percent_radio_2, number_of_song_radio_2
// MAGIC FROM nrjnova n1
// MAGIC JOIN nrjnova n2
// MAGIC ON n1.radio < n2.radio AND LOWER(n1.artist) =  LOWER(n2.artist) AND  LOWER(n1.title) =  LOWER(n2.title)-- thanks http://stackoverflow.com/a/13041833/3535853 for the "<" trick
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_1, radio
// MAGIC       FROM nrjnova n3
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal1
// MAGIC ON n1.radio = subTotal1.radio
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_2, radio
// MAGIC       FROM nrjnova n4
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal2
// MAGIC ON n2.radio = subTotal2.radio 
// MAGIC GROUP BY n1.radio, n2.radio, number_of_song_radio_1, number_of_song_radio_2
// MAGIC ORDER BY number_of_similar_songs DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DISTINCT n1.title, n1.artist, CONCAT(n1.radio, "-", n2.radio) as radios, number_of_song_radio_1, number_of_song_radio_2
// MAGIC FROM nrjnova n1
// MAGIC JOIN nrjnova n2
// MAGIC ON n1.radio < n2.radio AND n1.radio ="nova" AND LOWER(n1.artist) =  LOWER(n2.artist) AND  LOWER(n1.title) =  LOWER(n2.title)
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_1, radio
// MAGIC       FROM nrjnova n3
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal1
// MAGIC ON n1.radio = subTotal1.radio
// MAGIC JOIN (SELECT COUNT(DISTINCT title, artist) as number_of_song_radio_2, radio
// MAGIC       FROM nrjnova n4
// MAGIC       GROUP BY radio
// MAGIC      ) as subTotal2
// MAGIC ON n2.radio = subTotal2.radio 
// MAGIC ORDER BY radios

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT LOWER(title) as Title, LOWER(artist) as Artist,  COUNT(DISTINCT (radio))
// MAGIC FROM nrjnova n1
// MAGIC GROUP BY LOWER(title), LOWER(artist)
// MAGIC HAVING COUNT(DISTINCT (radio)) = (
// MAGIC                                   SELECT MAX (count)
// MAGIC                                         FROM (
// MAGIC                                           SELECT COUNT(DISTINCT (radio)) as count, LOWER(title), LOWER(artist)
// MAGIC                                           FROM nrjnova n1
// MAGIC                                           GROUP BY LOWER(title), LOWER(artist)
// MAGIC                                           HAVING COUNT(DISTINCT (radio))
// MAGIC                                        )
// MAGIC                                  )

// COMMAND ----------

// MAGIC %md
// MAGIC # When does Radios add new songs the most ?
// MAGIC We can find when Nova usually adds new tracks into their playlist if we spot when is the first time a track is broadcasted

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd')) as date
// MAGIC FROM nova                            
// MAGIC --WHERE artist = 'Anderson.paak'
// MAGIC GROUP BY  artist, title
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %md
// MAGIC # How many songs are added each day?
// MAGIC 
// MAGIC Note: it's normally to see a lot of new songs at first, because no songs were known

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(date) as new_songs_added, date, radio
// MAGIC FROM ( 
// MAGIC   SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd')) as date, radio
// MAGIC   FROM nrjnova    
// MAGIC   GROUP BY  artist, title, radio
// MAGIC )
// MAGIC GROUP BY date, radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(date) as new_songs_added, date, radio
// MAGIC FROM ( SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM')) as date, radio
// MAGIC FROM nrjnova                            
// MAGIC GROUP BY  artist, title, radio
// MAGIC       )
// MAGIC GROUP BY date, radio
// MAGIC ORDER BY date

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(new_songs_added) as average_new_song_per_day, radio
// MAGIC FROM (
// MAGIC   SELECT COUNT(date) as new_songs_added, date, radio
// MAGIC   FROM ( 
// MAGIC     SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM')) as date, radio
// MAGIC     FROM nrjnova                            
// MAGIC     GROUP BY  artist, title, radio
// MAGIC   )
// MAGIC   WHERE date > '2016-04'
// MAGIC   GROUP BY date, radio
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC -- thanks http://stackoverflow.com/questions/25006607/how-to-get-day-of-week-in-sparksql
// MAGIC SELECT COUNT( DATE_FORMAT(CAST(timestamp as timestamp),'EEEE')) as new_songs_added, DATE_FORMAT(CAST(timestamp as timestamp),'EEEE') as weekday, radio
// MAGIC FROM (
// MAGIC   SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd')) as date, timestamp, radio
// MAGIC   FROM nrjnova                            
// MAGIC   --WHERE artist = 'Anderson.paak'
// MAGIC   GROUP BY  artist, title,timestamp, radio
// MAGIC )
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'EEEE'), radio
// MAGIC ORDER BY weekday

// COMMAND ----------

// MAGIC %sql
// MAGIC -- thanks http://stackoverflow.com/questions/25006607/how-to-get-day-of-week-in-sparksql
// MAGIC SELECT COUNT( DATE_FORMAT(CAST(timestamp as timestamp),'HH')) as new_songs_added, DATE_FORMAT(CAST(timestamp as timestamp),'HH') as hour_of_the_day, radio
// MAGIC FROM ( 
// MAGIC     SELECT artist, title, MIN(DATE_FORMAT(CAST(timestamp as timestamp),'Y-MM-dd')) as date, timestamp, radio
// MAGIC     FROM nrjnova                            
// MAGIC     GROUP BY  artist, title,timestamp, radio
// MAGIC )
// MAGIC GROUP BY DATE_FORMAT(CAST(timestamp as timestamp),'HH'), radio
// MAGIC ORDER BY hour_of_the_day

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Spotify Part
// MAGIC Using the Spotify API to get metadata about the radio songs

// COMMAND ----------

case class SimpleSong(number_of_broadcast: Long, artist: String, title: String, radio: String)

// COMMAND ----------

val top100Nova = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova
WHERE radio = 'nova'
GROUP BY artist, title, radio
ORDER BY number_of_broadcast DESC
LIMIT 200
""").as[SimpleSong]
top100Nova.createOrReplaceTempView("nova100")


val top100NRJ = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova
WHERE radio = 'nrj'
GROUP BY artist, title, radio
ORDER BY number_of_broadcast DESC
LIMIT 200
""").as[SimpleSong]
top100NRJ.createOrReplaceTempView("nrj100")

val top100Virgin = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova
WHERE radio = 'virgin'
GROUP BY artist, title, radio
ORDER BY number_of_broadcast DESC
LIMIT 200
""").as[SimpleSong]
top100NRJ.createOrReplaceTempView("virgin100")

val top100Skyrock = sqlContext.sql("""
SELECT COUNT(*) as number_of_broadcast, artist, title, radio
FROM nrjnova
WHERE radio = 'skyrock'
GROUP BY artist, title, radio
ORDER BY number_of_broadcast DESC
LIMIT 200
""").as[SimpleSong]
top100Skyrock.createOrReplaceTempView("skyrock100")


// COMMAND ----------

display(top100Nova)

// COMMAND ----------

display(top100NRJ)

// COMMAND ----------

display(top100Virgin)

// COMMAND ----------

display(top100Skyrock)

// COMMAND ----------

import org.apache.http.impl.client.{BasicResponseHandler, DefaultHttpClient}
import org.apache.http.{HttpRequest, HttpRequestInterceptor}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.protocol.HttpContext
import org.apache.log4j.Logger
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity 

def simpleRequest(url: String): String = {
    val client = new DefaultHttpClient

    simpleRequest(client, url)
  }

  def simpleRequest(client: DefaultHttpClient, url: String): String = {
    try {
      val get = new HttpGet(url)

      val responseHandler = new BasicResponseHandler
      client.execute(get, responseHandler)
    } finally {
      client.getConnectionManager.shutdown()
    }
  }

  def simplePostRequest(client: DefaultHttpClient, url: String): String = {
    try {
      val post = new HttpPost(url)
      /* post.addHeader("appid","YahooDemo")
       post.addHeader("query","umbrella")
       post.addHeader("results","10")*/

      /*   val params = client.getParams
         params.setParameter("foo", "bar")

         val nameValuePairs = new ArrayList[NameValuePair](1)
         nameValuePairs.add(new BasicNameValuePair("registrationid", "123456789"));
         nameValuePairs.add(new BasicNameValuePair("accountType", "GOOGLE"));
         post.setEntity(new UrlEncodedFormEntity(nameValuePairs));*/

      // send the post request
      val responsePost = client.execute(post)
      println(responsePost.toString)
      "POST " + url

    } finally {
      client.getConnectionManager.shutdown()
    }
  }

  def requestWithHeaders(url: String, headers: Map[String,String], isPost : Boolean = false): String = {
    val client = new DefaultHttpClient

    try {
      client.addRequestInterceptor(new HttpRequestInterceptor {
        def process(request: HttpRequest, context: HttpContext) {
          for ((k,v) <- headers) {
            request.addHeader(k, v)
          }
        }
      })
      if(isPost) {
        simplePostRequest(client, url)
      } else {
        simpleRequest(client, url)
      }
    }
  }

// COMMAND ----------

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


val bearer =  ("Authorization" -> "Bearer BQCEagY141wCv8EpJg7Nk5ZTgRrgJJw6EbGtpsttNhyFttJUNbputtOcT-7VQ_hunfLyOjSFi3vLHVuJroEmmPH0eDFutAR2OgRzXfhpzpTfvGjSQhgNx0Nz0hOkbXeeHhWYSoh3fdpKrJM0utWUPSjt6Q-8wNnGCLfCGHuD_PI46GPD-MrfSv9X2KF1OLk6Bv3z0tsdwtdMmYJq3ElasRv3qT1UoqM5NsEMsxdZDt4xkA6LG46j5bGU8kyQ8hYaKzZXPODrOf1-06geMXhoIpuSHGMiVPcA7_jcQQ6Wp2U1k1SwJQ")

val headers = Map(
     "Accept" -> "application/json",
     bearer
)

//https://developer.spotify.com/web-api/console/get-search-item/
def getTrack(query: String) = {
  val encodedQuery = URLEncoder.encode(query, "UTF-8");
  try {
    println(s"https://api.spotify.com/v1/search?q=$encodedQuery&type=track&limit=1")
    val result = requestWithHeaders(s"https://api.spotify.com/v1/search?q=$encodedQuery&type=track&limit=1",headers)
    result
  } catch {
    case e: Exception =>
      println(e.toString)
      """
      
      {}
      
      """
  }
}

def getAll() = {
  (0 to 5000 by 50).map { offset => 
    requestWithHeaders(s"https://api.spotify.com/v1/me/tracks?limit=50&offset=$offset",headers)
  }
}

//top 100 year : 37i9dQZF1Cz3UIx50AV90C //@TODO
def getPlaylist(id: String = "6nEB4ldXLl97esUlqsufpI") = {
  (0 to 100 by 50).map { offset => 
    requestWithHeaders(s"https://api.spotify.com/v1/playlist/$id?limit=50&offset=$offset",headers)
  }
}

//Get audio feature of a track
def getAudioFeatureByTrack(id: String) = {
  requestWithHeaders(s"https://api.spotify.com/v1/audio-features/$id", headers)
}

def getArtist(id: String) = {
  requestWithHeaders(s"https://api.spotify.com/v1/artists/$id", headers)
}

//Radio Nova tOP 2016 : https://play.spotify.com/user/cpolos/playlist/4IaFC5nSCpejtZ0Ob0DhUe 
def addTracksPlaylist(uris: List[String], idPlaylist : String = "4IaFC5nSCpejtZ0Ob0DhUe") = {
  //As I am too lazy to see the uris list limi, i add the track one by one to the playlist
  uris.map( uri => {
    val encodedUri = URLEncoder.encode(uri, "UTF-8");
    val test = requestWithHeaders(s"https://api.spotify.com/v1/users/cpolos/playlists/$idPlaylist/tracks?uris=$encodedUri", headers, isPost = true)
    println(test)
  })
}


// COMMAND ----------

/*val spotifiedNovaSongs = spark.sparkContext.makeRDD(
  top100Nova.collect().map(song => {
    val artist = song.artist
    val title = song.title
    val a = getTrack(s"$artist  $title")
    println(a)
    a
}).toList 
)
val spotifiedNovaSongsDF = spark.read.json(spotifiedNovaSongs)
spotifiedNovaSongsDF.printSchema
spotifiedNovaSongsDF.createOrReplaceTempView("novaSpotify")
spotifiedNovaSongsDF.write.parquet("spotifiedNovaSongs_20170211.parquet")*/

// COMMAND ----------

val spotifiedNovaSongsDF = spark.read.parquet("spotifiedNovaSongs_20170211.parquet")
spotifiedNovaSongsDF.printSchema
spotifiedNovaSongsDF.createOrReplaceTempView("novaSpotify")

// COMMAND ----------

/*val spotifiedNRJSongs = spark.sparkContext.makeRDD(
  top100NRJ.collect().map(song => {
    val artist = song.artist
    val title = song.title
    val a = getTrack(s"$artist  $title")
    println(a)
    a
}).toList 
)
val spotifiedNRJSongsDF = spark.read.json(spotifiedNRJSongs)
spotifiedNRJSongsDF.printSchema
spotifiedNRJSongsDF.createOrReplaceTempView("nrjSpotify")
spotifiedNRJSongsDF.write.parquet("spotifiedNRJSongs_20170211.parquet")
*/

// COMMAND ----------

/*val spotifiedVirginSongs = spark.sparkContext.makeRDD(
  top100Virgin.collect().map(song => {
    val artist = song.artist
    val title = song.title
    val a = getTrack(s"$artist  $title")
    println(a)
    a
}).toList 
)
val spotifiedVirginSongsDF = spark.read.json(spotifiedVirginSongs)
spotifiedVirginSongsDF.printSchema
spotifiedVirginSongsDF.createOrReplaceTempView("virginSpotify")
spotifiedVirginSongsDF.write.parquet("spotifiedVirginSongs.parquet") //3ySBdFmKboUlMWfqSpLZio
*/

val spotifiedVirginSongsDF = spark.read.parquet("spotifiedVirginSongs.parquet")
spotifiedVirginSongsDF.createOrReplaceTempView("virginSpotify")

// COMMAND ----------

val spotifiedNRJSongsDF = spark.read.parquet("spotifiedNRJSongs_20170211.parquet")
spotifiedNRJSongsDF.createOrReplaceTempView("nrjSpotify")

// COMMAND ----------

/*
  val spotifiedSkyrockSongs = spark.sparkContext.makeRDD(
    top100Skyrock.collect().map(song => {
      val artist = song.artist
      val title = song.title
      val a = getTrack(s"$artist  $title")
      println(a)
      a
  }).toList 
  )
  val spotifiedSkyrockSongsDF = spark.read.json(spotifiedSkyrockSongs)
  spotifiedSkyrockSongsDF.printSchema
  spotifiedSkyrockSongsDF.createOrReplaceTempView("skyrockSpotify")
  spotifiedSkyrockSongsDF.write.parquet("spotifiedSkyrockSongs.parquet") //3ySBdFmKboUlMWfqSpLZio
  */

 val spotifiedSkyrockSongsDF = spark.read.parquet("spotifiedSkyrockSongs.parquet")
spotifiedSkyrockSongsDF.createOrReplaceTempView("skyrockSpotify")

// COMMAND ----------

// MAGIC %md
// MAGIC # Top Songs Playlists
// MAGIC Create Playlist for Top 200 songs broadcasted on both radio:
// MAGIC * [https://play.spotify.com/user/cpolos/playlist/7ECHPNZ5fjLggN2rg0poK3](NRJ)
// MAGIC * [https://play.spotify.com/user/cpolos/playlist/64DWZ46dFb50FaWs9eALIu](Nova)

// COMMAND ----------


val urisNRJ = sqlContext.sql("""
SELECT tracks.items.uri[0] as uri
FROM nrjSpotify
WHERE tracks.items.uri[0] IS NOT NULL
""").as[String].collect().toList

//addTracksPlaylist(urisNRJ, "7ECHPNZ5fjLggN2rg0poK3")

// COMMAND ----------


val urisNova = sqlContext.sql("""
SELECT tracks.items.uri[0] as uri
FROM novaSpotify
WHERE tracks.items.uri[0] IS NOT NULL
""").as[String].collect().toList

//addTracksPlaylist(urisNova, "64DWZ46dFb50FaWs9eALIu")

// COMMAND ----------


val urisVirgin = sqlContext.sql("""
SELECT tracks.items.uri[0] as uri
FROM virginSpotify
WHERE tracks.items.uri[0] IS NOT NULL
""").as[String].collect().toList

//addTracksPlaylist(urisVirgin, "3ySBdFmKboUlMWfqSpLZio")

// COMMAND ----------

val urisSkyrock = sqlContext.sql("""
SELECT tracks.items.uri[0] as uri
FROM skyrockSpotify
WHERE tracks.items.uri[0] IS NOT NULL
""").as[String].collect().toList

//addTracksPlaylist(urisSkyrock, "0x2SJgvfcBzxfiMTDkTuC4")

// COMMAND ----------

/*val onlyIds = sqlContext.sql("""
SELECT DISTINCT(tracks.items.id[0]) as id
FROM novaSpotify
WHERE tracks.items.id[0] IS NOT NULL
""")

val trackAudioFeatures = onlyIds.collect().map(row => {
  val trackId = row.getAs[String]("id")  
  getAudioFeatureByTrack(trackId)
})

val audioFeatures = spark.sparkContext.makeRDD(
  trackAudioFeatures
)
val audioFeaturesDF = spark.read.json(audioFeatures)
audioFeaturesDF.createOrReplaceTempView("audioFeature")*/

// COMMAND ----------

/*val onlyIdsNRJ = sqlContext.sql("""
SELECT DISTINCT(tracks.items.id[0]) as id
FROM nrjSpotify
WHERE tracks.items.id[0] IS NOT NULL
""")

val trackAudioFeaturesNRJ = onlyIdsNRJ.collect().map(row => {
  val trackId = row.getAs[String]("id")  
  getAudioFeatureByTrack(trackId)
})

val audioFeaturesNRJ = spark.sparkContext.makeRDD(
  trackAudioFeaturesNRJ
)
val audioFeaturesDFNRJ = spark.read.json(audioFeaturesNRJ)
audioFeaturesDFNRJ.createOrReplaceTempView("audioFeatureNRJ")
display(audioFeaturesDFNRJ)*/

// COMMAND ----------

/*val onlyIdsVirgin = sqlContext.sql("""
SELECT DISTINCT(tracks.items.id[0]) as id
FROM virginSpotify
WHERE tracks.items.id[0] IS NOT NULL
""")

    val trackAudioFeaturesVirgin = onlyIdsVirgin.collect().map(row => {
      val trackId = row.getAs[String]("id")
      getAudioFeatureByTrack(trackId)
    })

    val audioFeaturesVirgin = spark.sparkContext.makeRDD(
      trackAudioFeaturesVirgin
    )
    val audioFeaturesDFVirgin = spark.read.json(audioFeaturesVirgin)
    audioFeaturesDFVirgin.createOrReplaceTempView("audioFeatureVirgin")
    display(audioFeaturesDFVirgin)
*/

// COMMAND ----------

/* val onlyIdsSkyrock = sqlContext.sql("""
SELECT DISTINCT(tracks.items.id[0]) as id
FROM skyrockSpotify
WHERE tracks.items.id[0] IS NOT NULL
""")

  val trackAudioFeaturesSkyrock = onlyIdsSkyrock.collect().map(row => {
    val trackId = row.getAs[String]("id")
    getAudioFeatureByTrack(trackId)
  })

  val audioFeaturesSkyrock = spark.sparkContext.makeRDD(
    trackAudioFeaturesSkyrock
  )
  val audioFeaturesDFSkyrock = spark.read.json(audioFeaturesSkyrock)
  audioFeaturesDFSkyrock.createOrReplaceTempView("audioFeatureSkyrock")
  display(audioFeaturesDFSkyrock)*/

// COMMAND ----------

/*val trackArtists = sqlContext.sql("""
SELECT DISTINCT(tracks.items.artists[0].id[0]) as artistId
FROM novaSpotify
WHERE tracks.items.artists[0].id[0] IS NOT NULL
"""
).collect().map(row => {
  val artistId = row.getAs[String]("artistId")
  getArtist(artistId)
})

val artists = spark.sparkContext.makeRDD(
  trackArtists
)

val artistsDF = spark.read.json(artists)
artistsDF.createOrReplaceTempView("artist")
display(artistsDF)*/

// COMMAND ----------

/*val trackArtistsNRJ = sqlContext.sql("""
SELECT DISTINCT(tracks.items.artists[0].id[0]) as artistId
FROM nrjSpotify
WHERE tracks.items.artists[0].id[0] IS NOT NULL
"""
).collect().map(row => {
  val artistId = row.getAs[String]("artistId")
  getArtist(artistId)
})

val artistsNRJ = spark.sparkContext.makeRDD(
  trackArtistsNRJ
)

val artistsDFNRJ = spark.read.json(artistsNRJ)
artistsDFNRJ.createOrReplaceTempView("artistNRJ")
display(artistsDFNRJ)*/

// COMMAND ----------

/*val trackArtistsVirgin = sqlContext.sql("""
SELECT DISTINCT(tracks.items.artists[0].id[0]) as artistId
FROM virginSpotify
WHERE tracks.items.artists[0].id[0] IS NOT NULL
"""
    ).collect().map(row => {
      val artistId = row.getAs[String]("artistId")
      getArtist(artistId)
    })

    val artistsVirgin = spark.sparkContext.makeRDD(
      trackArtistsVirgin
    )

    val artistsDFVirgin = spark.read.json(artistsVirgin)
    artistsDFVirgin.createOrReplaceTempView("artistVirgin")
    display(artistsDFVirgin)*/

// COMMAND ----------

/* val trackArtistsSkyrock = sqlContext.sql("""
SELECT DISTINCT(tracks.items.artists[0].id[0]) as artistId
FROM skyrockSpotify
WHERE tracks.items.artists[0].id[0] IS NOT NULL
"""
  ).collect().map(row => {
    val artistId = row.getAs[String]("artistId")
    getArtist(artistId)
  })

  val artistsSkyrock = spark.sparkContext.makeRDD(
    trackArtistsSkyrock
  )

  val artistsDFSkyrock = spark.read.json(artistsSkyrock)
  artistsDFSkyrock.createOrReplaceTempView("artistSkyrock")
  display(artistsDFSkyrock)*/

// COMMAND ----------

/*val maxiJoin = sqlContext.sql("""
SELECT *, 'nova' as radio
FROM novaSpotify s
JOIN audioFeature a
ON a.id = s.tracks.items.id[0]
JOIN artist r
ON r.id = s.tracks.items.artists[0].id[0]
"""
)
val dropJoin = maxiJoin.drop("uri").drop("id").drop("type") //duplicate column
dropJoin.createOrReplaceTempView("AudioFeatureArtistTrack")
dropJoin.printSchema
dropJoin.write.parquet("novaTop200_11022017_2.parquet")
*/
val dropJoin = spark.read.parquet("novaTop200_11022017_2.parquet")

// COMMAND ----------

/*val maxiJoinNRJ = sqlContext.sql("""
SELECT *, 'nrj' as radio
FROM nrjSpotify s
JOIN audioFeatureNRJ a
ON a.id = s.tracks.items.id[0]
JOIN artistNRJ r
ON r.id = s.tracks.items.artists[0].id[0]
"""
)
//maxiJoin.drop("uri").drop("id") //duplicate column
val dropJoinNRJ = maxiJoinNRJ.drop("uri").drop("id").drop("type")
dropJoinNRJ.createOrReplaceTempView("AudioFeatureArtistTrackNRJ")
dropJoinNRJ.printSchema
dropJoinNRJ.write.parquet("nrjTop200_11022017_3.parquet")
*/
val dropJoinNRJ = spark.read.parquet("nrjTop200_11022017_3.parquet")

// COMMAND ----------

 /*val maxiJoinVirgin = sqlContext.sql("""
SELECT *, 'virgin' as radio
FROM virginSpotify s
JOIN audioFeatureVirgin a
ON a.id = s.tracks.items.id[0]
JOIN artistVirgin r
ON r.id = s.tracks.items.artists[0].id[0]
"""
    )
    //maxiJoin.drop("uri").drop("id") //duplicate column
    val dropJoinVirgin = maxiJoinVirgin.drop("uri").drop("id").drop("type")
    dropJoinVirgin.createOrReplaceTempView("AudioFeatureArtistTrackVirgin")
    dropJoinVirgin.printSchema
    dropJoinVirgin.write.parquet("virginTop200_16-02-2017.parquet")*/

val dropJoinVirgin = spark.read.parquet("virginTop200_16-02-2017.parquet")

// COMMAND ----------

/* val maxiJoinSkyrock = sqlContext.sql("""
SELECT *, 'skyrock' as radio
FROM skyrockSpotify s
JOIN audioFeatureSkyrock a
ON a.id = s.tracks.items.id[0]
JOIN artistSkyrock r
ON r.id = s.tracks.items.artists[0].id[0]
"""
  )
  //maxiJoin.drop("uri").drop("id") //duplicate column
  val dropJoinSkyrock = maxiJoinSkyrock.drop("uri").drop("id").drop("type")
  dropJoinSkyrock.createOrReplaceTempView("AudioFeatureArtistTrackSkyrock")
  dropJoinSkyrock.printSchema
  dropJoinSkyrock.write.parquet("skyrockTop200_16-02-2017.parquet")*/

val dropJoinSkyrock = spark.read.parquet("skyrockTop200_16-02-2017.parquet")

// COMMAND ----------

val AudioFeatureArtistTrackTmp = dropJoinNRJ.union(dropJoin)

// COMMAND ----------

val AudioFeatureArtistTrack= AudioFeatureArtistTrackTmp.union(dropJoinVirgin).union(dropJoinSkyrock)

// COMMAND ----------

 
/*val TrackArtistAudioFeature = sqlContext.sql("""
SELECT *
FROM TrackArtistAudioFeature
"""
)

TrackArtistAudioFeature.createOrReplaceTempView("AudioFeatureArtistTrackRadios")
TrackArtistAudioFeature.write.parquet("TrackArtistAudioFeature-01-03-2017.parquet")
*/


// COMMAND ----------

//AudioFeatureArtistTrack.createOrReplaceTempView("AudioFeatureArtistTrackRadios")
val TrackArtistAudioFeature = spark.read.parquet("TrackArtistAudioFeature-01-03-2017.parquet")
TrackArtistAudioFeature.cache()
TrackArtistAudioFeature.createOrReplaceTempView("AudioFeatureArtistTrackRadios")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) as number_of_songs, name as artist, popularity, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC GROUP BY name, popularity, radio
// MAGIC ORDER BY popularity DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND( (COUNT(t.*) / subTotal.total_radio * 100),2) AS percentage_of_songs, subTotal.total_radio,  ROUND(popularity / 10) * 10 AS popularity, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT COUNT(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio, ROUND(popularity / 10) * 10, t.radio
// MAGIC ORDER BY popularity

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(popularity),1), radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %md
// MAGIC # Musical Genres
// MAGIC In spotify, each songs can have several genres, rock, hip hop etc.
// MAGIC 
// MAGIC ## Spotify's genres
// MAGIC They are stored in an array this way :
// MAGIC ```
// MAGIC ["alternative dance","chamber pop","dance-punk","electronic","garage rock","indie pop","indie r&b","indie rock","indietronica","new rave","synthpop"]
// MAGIC ``` 
// MAGIC 
// MAGIC ## How to count genres properly
// MAGIC In order to know what are the favorite radio genres, we need to explode this array and count each genre individuality.

// COMMAND ----------

import org.apache.spark.sql.functions.explode
//We have Array[Array] so we need to explode it
val genres = TrackArtistAudioFeature.select($"name", explode($"genres"), $"tracks.name",$"radio").toDF("artist", "genres","title","radio")
genres.createOrReplaceTempView("genres")
genres.cache()
genres.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC # Genre's explode results
// MAGIC As you can see in the next cell, we added **a row for each song's genres** in our dataset in order to count them individuality

// COMMAND ----------

display(genres)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(genres) as number_of_songs_with_this_genre, genres, radio
// MAGIC FROM genres
// MAGIC GROUP BY genres, radio
// MAGIC ORDER BY number_of_songs_with_this_genre DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(genres) as number_of_songs_with_this_genre, genres, radio
// MAGIC FROM genres
// MAGIC GROUP BY genres, radio
// MAGIC ORDER BY number_of_songs_with_this_genre DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(genres) as number_of_hiphop_songs, radio
// MAGIC FROM genres
// MAGIC WHERE genres LIKE '%rap%' OR genres LIKE '%hip%' OR genres LIKE '%hop%'
// MAGIC GROUP BY radio
// MAGIC ORDER BY number_of_hiphop_songs DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(genres) as number_of_hip_hop_songs, genres, radio
// MAGIC FROM genres
// MAGIC WHERE genres LIKE '%rap%' OR genres LIKE '%hip%' OR genres LIKE '%hop%'
// MAGIC GROUP BY genres, radio
// MAGIC HAVING COUNT(genres) > 50
// MAGIC ORDER BY number_of_hip_hop_songs DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(DISTINCT genres) as number_of_genres, radio
// MAGIC FROM genres
// MAGIC GROUP BY radio
// MAGIC ORDER BY number_of_genres DESC

// COMMAND ----------

// MAGIC %md
// MAGIC # Visualize audio features
// MAGIC Let's see what are the songs with the most acousticness, danceability, energy, instrumentalness, liveness, loudness, valence value.
// MAGIC 
// MAGIC The full list of the audio features is here :
// MAGIC * https://developer.spotify.com/web-api/get-audio-features/#tablepress-215

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) AS number_of_songs, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT energy, tracks.name, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY energy DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT energy, tracks.name, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY energy
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT danceability, tracks.name, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY danceability DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT danceability, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY danceability 
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT acousticness, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY acousticness DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT acousticness, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY acousticness 
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC SELECT liveness, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY liveness DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT liveness, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY liveness 
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT valence , tracks.name[0] as title, name as artist, radio, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY valence  DESC
// MAGIC LIMIT 100

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT valence , tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY valence 
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT loudness, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY loudness DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT loudness, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY loudness
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT instrumentalness, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY instrumentalness DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT instrumentalness, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY instrumentalness
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT tempo, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY tempo DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT tempo, tracks.name[0] as title, name as artist
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY tempo
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((duration_ms / 1000 ) / 60,1) as minute, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY duration_ms DESC
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT  ROUND((duration_ms / 1000 ) / 60,1) as minute, tracks.name[0] as title, name as artist, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC ORDER BY duration_ms
// MAGIC LIMIT 50

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT FLOOR(12345.7344);

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND( (COUNT(t.*) / subTotal.total_radio * 100),2) AS percentage_of_songs, subTotal.total_radio, FLOOR((duration_ms / 1000 ) / 60) AS minute, ROUND( (((duration_ms / 1000 ) % 60)) / 10) * 10 AS seconds, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC WHERE ROUND((duration_ms / 1000 ) / 60) < 9
// MAGIC GROUP BY subTotal.total_radio, FLOOR((duration_ms / 1000 ) / 60), ROUND( (((duration_ms / 1000 ) % 60)) / 10) * 10, t.radio
// MAGIC ORDER BY minute, seconds -- TODO

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND( (COUNT(t.*) / subTotal.total_radio * 100),2) AS percentage_of_songs, subTotal.total_radio, FLOOR((duration_ms / 1000 ) / 60) AS minute, ROUND( (((duration_ms / 1000 ) % 60)) / 10) * 10 AS seconds, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC WHERE ROUND((duration_ms / 1000 ) / 60) < 9
// MAGIC GROUP BY subTotal.total_radio, FLOOR((duration_ms / 1000 ) / 60), ROUND( (((duration_ms / 1000 ) % 60)) / 10) * 10, t.radio
// MAGIC ORDER BY minute, seconds -- TODO

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG ( (duration_ms / 1000  / 60) + ROUND( (((duration_ms / 1000 ) % 60)) / 10) / 10) AS duration_minute, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC In my sense, the most meaningful audiofeaztures are :
// MAGIC * tempo
// MAGIC * danceability
// MAGIC * energy
// MAGIC * valence (positiveness)
// MAGIC 
// MAGIC Let's see their distribution among the radio's tracks

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((COUNT(t.*) / subTotal.total_radio * 100),2) AS percentage_of_songs, subTotal.total_radio, ROUND(tempo / 10) * 10 as rounded_tempo, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio, ROUND(tempo / 10) * 10, t.radio
// MAGIC ORDER BY rounded_tempo

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((COUNT(t.*) / subTotal.total_radio * 100),1) AS percentage_of_songs, subTotal.total_radio, ROUND(danceability * 10) / 10 as danceability, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio,  ROUND(danceability * 10) / 10, t.radio
// MAGIC ORDER BY danceability

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((COUNT(t.*) / subTotal.total_radio * 100),1) AS percentage_of_songs, subTotal.total_radio, ROUND(danceability * 10) / 10 as danceability, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio,  ROUND(danceability * 10) / 10, t.radio
// MAGIC ORDER BY danceability

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(danceability),2) as avg_danceability, radio
// MAGIC FROM (
// MAGIC   SELECT COUNT(*) as number_of_song, ROUND(danceability * 10) / 10 as danceability, radio
// MAGIC   FROM AudioFeatureArtistTrackRadios
// MAGIC   GROUP BY ROUND(danceability * 10) / 10, radio
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((COUNT(t.*) / subTotal.total_radio * 100),1) AS percentage_of_songs, subTotal.total_radio, ROUND(t.valence * 10) / 10 as valence, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio, ROUND(valence * 10) / 10, t.radio
// MAGIC ORDER BY valence

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND(AVG(valence), 2), radio
// MAGIC FROM (
// MAGIC SELECT COUNT(*) as number_of_song, ROUND(valence * 10) / 10 as valence, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC GROUP BY ROUND(valence * 10) / 10, radio
// MAGIC )
// MAGIC GROUP BY radio

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT ROUND((COUNT(t.*) / subTotal.total_radio * 100),1) as percentage_of_songs, subTotal.total_radio, ROUND(energy * 10) / 10 as energy, t.radio
// MAGIC FROM AudioFeatureArtistTrackRadios t
// MAGIC JOIN (
// MAGIC     SELECT count(*) AS total_radio, radio
// MAGIC     FROM AudioFeatureArtistTrackRadios
// MAGIC     GROUP BY radio
// MAGIC ) AS subTotal
// MAGIC ON subTotal.radio = t.radio
// MAGIC GROUP BY subTotal.total_radio, ROUND(energy * 10) / 10, t.radio
// MAGIC ORDER BY energy

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AVG(energy), radio
// MAGIC FROM (
// MAGIC SELECT COUNT(*) as number_of_song, ROUND(energy * 10) / 10 as energy, radio
// MAGIC FROM AudioFeatureArtistTrackRadios
// MAGIC GROUP BY ROUND(energy * 10) / 10, radio
// MAGIC )
// MAGIC GROUP BY radio
