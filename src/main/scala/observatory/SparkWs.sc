import java.time.LocalDate

import observatory.Location
import org.apache.spark.sql.{SparkSession}
import org.apache.log4j.{Level, Logger}
val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.ERROR)

val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("worksheet")
  .getOrCreate()

case class Station(stn: String, wban: String, lat: Double, lon: Double) {
  val location = Location(lat, lon)
}
case class Record(stn: String, wban: String, year: Int, month: Int, day: Int, temperature: Double) {
  val date = LocalDate.of(year, month, day)
}

val stationsFile = "D:\\Work\\coursera\\scala-assignments\\observatory\\src\\main\\resources\\stations.csv"
val recordsFile = "D:\\Work\\coursera\\scala-assignments\\observatory\\src\\main\\resources\\1975.csv"

val stations = spark.sparkContext.textFile(stationsFile)
  .map(_.split(","))
  .filter(_.length == 4)
  .map(i => Station(i(0), i(1), i(2).toDouble, i(3).toDouble))
  .filter(i => i.lon != 0.0 || i.lat != 0.0)
  .map(i => (i.stn, i))

val records = spark.sparkContext.textFile(recordsFile)
  .map(_.split(","))
  .map(i => Record(i(0), i(1), 1975, i(2).toInt, i(3).toInt, (i(4).toDouble - 32) * 5 / 9))
  .map(i => (i.stn, i))

val result = stations.join(records)
  .map(i => (i._2._2.date, i._2._1.location, i._2._2.temperature)).collect()

result.take(10).foreach(println)

val recordsRDD = spark.sparkContext.parallelize(result.toSeq)

val result2 = recordsRDD.map(i => (i._2, i._3))
  .map{ case (loc, tmp) => (loc, (tmp, 1)) }
  .reduceByKey{ case ((v1, c1), (v2, c2)) => (v1 + v2, c1 + c2) }
  .mapValues{ case (v, c) => v.toDouble / c.toDouble}
  .collect

result2.take(10).foreach(println)
