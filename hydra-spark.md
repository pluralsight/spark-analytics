```%dep

z.reset()
z.addRepo("jitpack.io").url("https://jitpack.io")
z.load("com.github.pluralsight.hydra-spark:hydra-spark-core_2.11:53cb35070a")





    import hydra.spark.api._
import hydra.spark.dispatch.SparkDispatch

val dispatch = SparkDispatch(dsl)

dispatch.validate


dispatch.run()
```