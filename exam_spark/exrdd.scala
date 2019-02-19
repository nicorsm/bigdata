
// Define the FlightData structure with the data needed
case class FlightData(month: Integer, departure: Integer, arrival: Integer, origin: String, destination: String)

// Define sunset and sunrise hours + "na" value outside these bounds to discard flights with "NA" values
val sunset = 2100
val sunrise = 600
val na = 1200
def isNumber(x: String) = x forall Character.isDigit

//Original path hdfs:/user/ngiancecchi/dataset/2007.csv

val rddFlights = spark.read.format("csv").option("header", "true").load("hdfs://localhost:8020/user/cloudera/dataset/2007.csv").rdd.map { x => 
	val month = x.getString(1).toInt
	val depString = x.getString(4)
	val arrString = x.getString(6)
	val departure = if(isNumber(depString)) depString.toInt else na
	val arrival = if(isNumber(arrString)) arrString.toInt else na
	FlightData(month,departure,arrival,x.getString(16),x.getString(17))
}

// Number of movements are considered as the sum of landings and takeoffs to/from an airport,
// so we need to consider the origin airport and the departure time for the first case,
// and destination airport and arrival time for the second case.
// Then we join both together.

// Pick just the summer months
val summerFlights = rddFlights.filter(_.month >= 6).filter(_.month <= 9)

// and filter flights with departure time in the night (sunset <= t <= sunrise)
val depAfterSunset = summerFlights.filter(_.departure >= sunset)
val depBeforeSunrise = summerFlights.filter(_.departure <= sunrise)

// join the two RDDs and maps to kv-pairs like ("SFO", 1), ("LAX", 1)
val departures = depAfterSunset.union(depBeforeSunrise).map(x => (x.origin, 1))


// Same here with arrivals...
val arrAfterSunset = summerFlights.filter(_.arrival >= sunset)
val arrBeforeSunrise = summerFlights.filter(_.arrival <= sunrise)
val arrivals = arrAfterSunset.union(arrBeforeSunrise).map(x => (x.destination, 1))

// Take the first 10 airports in the rank and print out on screen.
val flights = departures.union(arrivals).reduceByKey(_ + _)


val airports = spark.read.format("csv").option("header", "true").load("hdfs://localhost:8020/user/cloudera/dataset/airports.csv").rdd.map(x => (x.getString(0), x.getString(1)))

//
// Filters just the data we need: the month, departure and arrival times and airports
// Times are in a integer format (e.g. "950" for 9:50, "2310" for 23:10), so we can simply consider them as integer.

val results = flights.join(airports).takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2._1))
val output = Array[String]()
results.foreach { x =>
	println(x)
	output :+ x
}

import java.io.{File, PrintWriter}
val pw = new PrintWriter(new File("exrdd-output"))
pw.write(output.mkString("\n"))
pw.close()

///list.join(rddAirports).take(10).foreach(println)
///uso di repartition e coalesce + persistenza della cache

