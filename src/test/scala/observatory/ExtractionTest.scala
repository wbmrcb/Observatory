package observatory

import java.time.LocalDate

import org.scalatest.FunSuite

class ExtractionTest extends FunSuite {

  val result: Iterable[(LocalDate, Location, Double)] = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
  val combined: Iterable[(Location, Double)] = Extraction.locationYearlyAverageRecords(result)

  test("testLocateTemperatures") {
    assert(result.size === 3)
  }

  test("testLocationYearlyAverageRecords") {
    assert(combined.size === 2)
  }

}
