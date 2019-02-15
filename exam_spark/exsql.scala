
// Define the FlightData structure with the data needed
//case class FlightData(Month: Integer, DepTime: Integer, ArrTime: Integer, Origin: String, Dest: String)

// Define sunset and sunrise hours + "na" value outside these bounds to discard flights with "NA" values
val sunset = 2100
val sunrise = 600
val na = 1200

//val rddData = sc.textFile("hdfs:/user/ngiancecchi/dataset/2007.csv")  // Loads data from HDFS
//val header = rddData.first() // Picks the header (should not be considered when filtering)

// AGGIUNGERE SCHEMA!!!!!!

// Filters just the data we need: the month, departure and arrival times and airports
// Times are in a integer format (e.g. "950" for 9:50, "2310" for 23:10), so we can simply consider them as integer.

//val schema = Encoders.product[FlightData].schema
//val dfFlights = spark.read.format("csv").option("header", "true").schema(schema).load("hdfs:/user/ngiancecchi/dataset/2007.csv").select(

val dfFlights = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/2007.csv").select($"Month".alias("month"),$"Origin".alias("origin"),$"Dest".alias("destination"),$"DepTime".alias("departure"),$"ArrTime".alias("arrival"))
//dfFlights.select($"Month".alias("month"),$"DepTime".alias("departure"),$"ArrTime".alias("arrival"),$"Origin".alias("origin"),$"Dest".alias("destination")) 
//dfFlights.show()

//.map { x => 
//	val depString = x.getString(4)
//	val arrString = x.getString(6)
//	val departure = if(depString == "NA") na else depString.toInt
//	val arrival = if(arrString == "NA") na else arrString.toInt
//	FlightData(x.getInt(1),departure,arrival,x.getString(16),x.getString(17))
//}



// Number of movements are considered as the sum of landings and takeoffs to/from an airport,
// so we need to consider the origin airport and the departure time for the first case,
// and destination airport and arrival time for the second case.
// Then we join both together.

// Pick just the summer months
val summerFlights = dfFlights.where($"month".between(6,9))

// and filter flights with departure time in the night (sunset <= t <= sunrise),
// then group by origin airport and count the number of departures
val departures = summerFlights.select($"origin",$"departure").where($"departure" >= sunset || $"departure" <= sunrise).groupBy($"origin".alias("code")).count().withColumnRenamed("count","departures")
// Same here with arrivals...
val arrivals = summerFlights.select($"destination",$"arrival").where($"arrival" >= sunset || $"arrival" <= sunrise).groupBy($"destination".alias("code")).count().withColumnRenamed("count","arrivals")
//val rddAirports = sc.textFile("hdfs:/user/ngiancecchi/dataset/airports.csv")  // Loads data from HDFS
val airports = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/airports.csv").select($"iata",$"airport")
//
// Now we can join departures and arrivals, and compute the total amount of arrivals + departures
// so we have rows like "SFO" | 9876, "LAX" | 12345), and then sort descending by number of movements.
departures.join(arrivals, "code").withColumn("movements", $"departures" + $"arrivals").sort($"movements".desc).limit(10).join(airports, $"code" === $"iata").select("code","airport","movements").show() //)
//

