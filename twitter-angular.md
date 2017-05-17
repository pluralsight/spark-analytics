# Add Twitter Lib

```scala
%dep
z.reset()
z.load("org.apache.bahir:spark-streaming-twitter_2.11:2.1.0")
```

# Create Leaflet Map
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
                L.marker([tweet.lat, tweet.lon]).bindPopup("<b>" + tweet.user + "</b><br>" + tweet.tweet).addTo(map);             
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

# Import Spark Streaming libraries

```scala
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
```

# Configure Twitter OAuth credentials
```scala
val consumerKey = System.getenv("TWITTER_CONSUMER_KEY")
val consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET")
val accessToken = System.getenv("TWITTER_API_TOKEN")
val accessTokenSecret = System.getenv("TWITTER_API_TOKEN_SECRET")

System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
System.setProperty("twitter4j.oauth.accessToken", accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
```

# Start (Spark) streaming of (Twitter) tweets

```scala
case class Tweet(user: String, tweet: String, lat: Double, lon: Double)

val ssc = new StreamingContext(sc, Seconds(2))

val tweets = TwitterUtils.createStream(ssc, None).window(Seconds(10))
tweets.foreachRDD(rdd => {
    val df = rdd
      .filter(_.getGeoLocation != null)
      .map(t => Tweet(t.getUser.getName, t.getText, t.getGeoLocation.getLatitude, t.getGeoLocation.getLongitude))
          
    var items = df.collect
    z.angularBind("data", items) 
})

ssc.start()
```

# Stop it
```scala
ssc.stop(true, true)
```