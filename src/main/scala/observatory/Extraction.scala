package observatory

import java.io.File
import java.time.LocalDate

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getRootLogger.setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("Observatory")
    .getOrCreate()

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    val stations = spark.sparkContext.textFile(getClass.getResource(stationsFile).toURI.toString)
      .map(_.split(","))
      .filter(_.length == 4)
      .map(i => Station(i(0), i(1), i(2).toDouble, i(3).toDouble))
      .filter(i => i.lon != 0.0 || i.lat != 0.0)
      .map(i => (i.stn, i))

    val records = spark.sparkContext.textFile(getClass.getResource(temperaturesFile).toURI.toString)
      .map(_.split(","))
      .map(i => Record(i(0), i(1), year, i(2).toInt, i(3).toInt, (i(4).toDouble - 32) * 5 / 9))
      .map(i => (i.stn, i))

    stations.join(records)
      .map(i => (i._2._2.date, i._2._1.location, i._2._2.temperature))
      .collect
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    val recordsRDD = spark.sparkContext.parallelize(records.toSeq)

    recordsRDD.map(i => (i._2, i._3))
      .map{ case (loc, tmp) => (loc, (tmp, 1)) }
      .reduceByKey{ case ((v1, c1), (v2, c2)) => (v1 + v2, c1 + c2) }
      .mapValues{ case (v, c) => v.toDouble / c.toDouble }
      .collect
  }

}
