import org.apache.spark.sql.SparkSession

// Define sunset and sunrise hours + "na" value outside these bounds to discard flights with "NA" values
val sunset = 2100
val sunrise = 600

val spark = SparkSession.builder().appName("Nightly Flights (SparkSQL)").master("local").getOrCreate

// Filters just the data we need: the month, departure and arrival times and airports
// Times are in a integer format (e.g. "950" for 9:50, "2310" for 23:10), so we can simply consider them as integer.

val data = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/2007.csv")

//val dfFlights = data.select($"Month".alias("month"),$"Origin".alias("origin"),$"Dest".alias("destination"),$"DepTime".alias("departure"),$"ArrTime".alias("arrival"))

// Number of movements are considered as the sum of landings and takeoffs to/from an airport,
// so we need to consider the origin airport and the departure time for the first case,
// and destination airport and arrival time for the second case.
// Then we join both together.

// Pick just the summer months
val summerFlights = data.where($"Month".between(6,9)).select($"Origin".alias("origin"),$"Dest".alias("destination"),$"DepTime".alias("departure"),$"ArrTime".alias("arrival"))
summerFlights.cache()

// and filter flights with departure time in the night (sunset <= t <= sunrise),
// then group by origin airport and count the number of departures

val departures = summerFlights.select($"origin",$"departure").where($"departure" >= sunset || $"departure" <= sunrise).groupBy($"origin".alias("code")).count().withColumnRenamed("count","departures")

// Same here with arrivals...

val arrivals = summerFlights.select($"destination",$"arrival").where($"arrival" >= sunset || $"arrival" <= sunrise).groupBy($"destination".alias("code")).count().withColumnRenamed("count","arrivals")

val airports = spark.read.format("csv").option("header", "true").load("hdfs:/user/ngiancecchi/dataset/airports.csv").select($"iata",$"airport")

// Now we can join departures and arrivals, and compute the total amount of arrivals + departures
// so we have rows like "SFO" | 9876, "LAX" | 12345), and then sort descending by number of movements.

val top10 = departures.join(arrivals, "code").withColumn("movements", $"departures" + $"arrivals").sort($"movements".desc).limit(10)

val results = top10.join(airports, $"code" === $"iata", "left").select("code","airport","movements")

results.write.format("csv").mode("overwrite").option("header","true").save("./exsql-output")

results.show()
summerFlights.unpersist()
