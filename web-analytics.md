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
    if (ip == null) null else Http("http://freegeoip.net/json/"+ip).execute().body
  }
  
  val geoLocateUDF = udf(geoLocate)
```

```scala
import org.apache.spark.sql.types._

val schema = new StructType().add("city", StringType).add("country_name",StringType).add("latitude",DoubleType).add("longitude",DoubleType)
 
val geodf = ndf.withColumn("geolocation",from_json(geoLocateUDF(ndf.col("ip")),schema))
```

```scala
geodf.select("geolocation").show(10,false)
```

```
case class GeoLocation(email: String, city: String, latitude: Double, longitude: Double)

val gdf = geodf.select("traits.email","geolocation.city","geolocation.latitude","geolocation.longitude")
            .filter(row=>row.get(2)!=null)
            .map(row=>GeoLocation(row.getString(0),row.getString(1),row.getDouble(2),row.getDouble(3)))
            
```

```scala
val items = gdf.collect
```

```javascript

```javascript
%angular
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.5/leaflet.css" />
<div id="map" style="height: 800px; width: 100%"></div>

<script type="text/javascript">
function initMap() {
    var map = L.map('map').setView([30.00, -30.00], 3);

    L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors',
        maxZoom: 12,
        minZoom: 3
    }).addTo(map);

    var el = angular.element($('#map').parent('.ng-scope'));
    angular.element(el).ready(function() {
        window.locationWatcher = el.scope().compiledScope.$watch('data', function(newValue, oldValue) {
            angular.forEach(newValue, function(tweet) {
                L.marker([tweet.latitude, tweet.longitude]).bindPopup("<b>" + tweet.city + "</b><br>" + tweet.email).addTo(map);             
            });
        })
    });
}

if (window.locationWatcher) { window.locationWatcher(); }

if (window.L) { initMap(); }
else {
    var sc = document.createElement('script');
    sc.type = 'text/javascript';
    sc.src = 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.5/leaflet.js';
    sc.onload = initMap;
    sc.onerror = function(err) { alert(err); }
    document.getElementsByTagName('head')[0].appendChild(sc);
}
</script>
```
