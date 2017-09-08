package observatory

import java.time.LocalDate

case class Location(lat: Double, lon: Double)

case class Color(red: Int, green: Int, blue: Int)

case class Station(stn: String, wban: String, lat: Double, lon: Double) {
  val id: (String, String) = (stn, wban)
  val location = Location(lat, lon)
}
case class Record(stn: String, wban: String, year: Int, month: Int, day: Int, temperature: Double) {
  val id: (String, String) = (stn, wban)
  val date: LocalDate = LocalDate.of(year, month, day)
}

