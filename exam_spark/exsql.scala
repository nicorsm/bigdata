case class FlightData(month: Integer, departure: Integer, arrival: Integer, origin: String, destination: String)

val sunset = 2100
val sunrise = 600
val na = 1200

val rddData = sc.textFile("hdfs:/user/cloudera/2007.csv")
val header = rddData.first()
val dfFlights = rddData.filter(x => x != header).map { ln =>
	val cols = ln.split(",")
	val departure = if(cols(4) == "NA") na else cols(4).toInt
	val arrival = if(cols(6) == "NA") na else cols(6).toInt
	FlightData(cols(1).toInt,departure,arrival,cols(16),cols(17))
}.toDF()

val summerFlights = dfFlights.where($"month".between(6,9))

val departures = summerFlights.select($"origin",$"departure").where($"departure" >= sunset || $"departure" <= sunrise).groupBy($"origin".alias("airport")).count().withColumnRenamed("count","departures")

val arrivals = summerFlights.select($"destination",$"arrival").where($"arrival" >= sunset || $"arrival" <= sunrise).groupBy($"destination".alias("airport")).count().withColumnRenamed("count","arrivals")

departures.join(arrivals, "airport").withColumn("movements", $"departures" + $"arrivals").drop($"departures").drop($"arrivals").sort($"movements".desc).limit(10).show()


