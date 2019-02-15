
// Define the FlightData structure with the data needed
case class FlightData(month: Integer, departure: Integer, arrival: Integer, origin: String, destination: String)

// Define sunset and sunrise hours + "na" value outside these bounds to discard flights with "NA" values
val sunset = 2100
val sunrise = 600
val na = 1200

val rddFlights = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/2007.csv").rdd.map { x => 
	val depString = x.getInt(4)
	val arrString = x.getInt(6)
	val departure = if(depString == "NA") na else depString.toInt
	val arrival = if(arrString == "NA") na else arrString.toInt
	FlightData(x.getInt(1),departure,arrival,x.getString(16),x.getString(17))
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

//val rddAirports = sc.textFile("hdfs:/user/ngiancecchi/dataset/airports.csv")  // Loads data from HDFS
val airports = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/airports.csv").rdd.map(x => (x.getString(0), x.getString(1)))
//
// Filters just the data we need: the month, departure and arrival times and airports
// Times are in a integer format (e.g. "950" for 9:50, "2310" for 23:10), so we can simply consider them as integer.
flights.join(airports).takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2._1)).foreach(println)

//results.foreach(println)
//results.saveAsTextFile("hdfs:/user/ngiancecchi/exam/results.txt")

//list.take(10).join(rddAirports).foreach(println)

//uso di repartition e coalesce + persistenza della cache
