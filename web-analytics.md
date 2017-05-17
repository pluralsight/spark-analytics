```scala
%dep
z.reset()
z.addRepo("jitpack.io").url("https://jitpack.io")
z.load("com.github.pluralsight.hydra-spark:hydra-spark-core_2.11:53cb35070a")
z.load("org.scalaj:scalaj-http_2.11:2.3.0")
```

```scala
    import hydra.spark.api._
import hydra.spark.dispatch.SparkDispatch

val dispatch = SparkDispatch(dsl)

dispatch.validate
```

```scala
dispatch.run()
```

```scala
%sql
select count(*) from identify_events
```

```scala
val df = spark.sql("select * from identify_events")
```

```scala
df.select("context.ip").show()
```

```scala

val ipFix = (s:String) => {
   if (s == null) null else s.split(",")(0)
}

val ipFixUdf = udf(ipFix)

val ndf = df.withColumn("ip", ipFixUdf(df.col("context.ip")) )
```

```scala
ndf.select("ip").show()
```

```scala
import scalaj.http.Http
  
  val geoLocate = (ip:String) => {
    if (ip == null) null else Http("http://ip-api.com/json/"+ip).execute().body
  }
  
  val geoLocateUDF = udf(geoLocate)
```

```scala
import org.apache.spark.sql.types._

val schema = new StructType().add("city", StringType).add("country",StringType).add("lat",DoubleType).add("lon",DoubleType)
 
val geodf = ndf.withColumn("geolocation",from_json(geoLocateUDF(ndf.col("ip")),schema))
```

```scala
geodf.select("geolocation").show(10,false)
```