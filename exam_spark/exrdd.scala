case class FlightData(month: Integer, departure: Integer, arrival: Integer, origin: String, destination: String)

val sunset = 2100
val sunrise = 600
val na = 1200

val rddData = sc.textFile("hdfs:/user/cloudera/2007.csv")
val header = rddData.first()
val rddFlights = rddData.filter(x => x != header).map { ln =>
	val cols = ln.split(",")
	val departure = if(cols(4) == "NA") na else cols(4).toInt
	val arrival = if(cols(6) == "NA") na else cols(6).toInt
	FlightData(cols(1).toInt,departure,arrival,cols(16),cols(17))
}

val summerFlights = rddFlights.filter(_.month >= 6).filter(_.month <= 9)

val depAfterSunset = summerFlights.filter(_.departure >= sunset).map(x => (x.origin, 1))
val depBeforeSunrise = summerFlights.filter(_.departure <= sunrise).map(x => (x.origin, 1))

val arrAfterSunset = summerFlights.filter(_.arrival >= sunset).map(x => (x.destination, 1))
val arrBeforeSunrise = summerFlights.filter(_.arrival <= sunrise).map(x => (x.destination, 1))

val list = (depAfterSunset ++ depBeforeSunrise ++ arrAfterSunset ++ arrBeforeSunrise).reduceByKey(_ + _).sortBy(_._2, false)
list.take(10).foreach(println)

