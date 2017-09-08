package observatory

import java.time.LocalDate

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

case class Station(stn: String, wban: String, lat: Double, lon: Double) {
  val location = Location(lat, lon)
}
case class Record(stn: String, wban: String, year: Int, month: Int, day: Int, temperature: Double) {
  val date = LocalDate.of(year, month, day)
}

