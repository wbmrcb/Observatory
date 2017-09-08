package observatory

import org.scalatest.FunSuite

class ExtractionTest extends FunSuite {

  test("testLocateTemperatures") {
    val result = Extraction.locateTemperatures(1975, "stations.csv", "1975.csv")

    assert(result.size > 1)
  }

}
