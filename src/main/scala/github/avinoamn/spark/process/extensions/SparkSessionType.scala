package github.avinoamn.spark.process.extensions

import github.avinoamn.geoJson.GeoJsonUDT
import org.apache.sedona.spark.SedonaContext
import org.apache.spark
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.types.{UDTRegistration, UserDefinedType}

import scala.util.Try

private[process] trait SparkSessionType extends UDTRegistration {

  implicit class SparkSessionTypeExtensions(t: SparkSession.type) {

    def build(processName: String,
              additionalConfigs: Map[String, Any],
              extensions: Seq[SparkSessionExtensions => Unit],
              enableHiveSupport: Boolean,
              useSedona: Boolean,
              userDefinedTypes: Seq[UserDefinedType[_]]): spark.sql.SparkSession = {

      // Create SparkSession builder
      var builder = (
        if (useSedona) SedonaContext.builder() else SparkSession.builder()
      ).appName(processName)

      // Set additional configs
      builder = builder.config(additionalConfigs)

      // Set extensions
      builder = extensions.foldLeft(builder) { case (builder, ext) => builder.withExtensions(ext) }

      // Set master
      builder = Try(sys.env("ENVIRONMENT")).getOrElse("PRODUCTION") match {
        case "DEVELOPMENT" => builder.master("local[*]")
        case "PRODUCTION" => builder
      }

      // Enable Hive support
      builder = if (enableHiveSupport) builder.enableHiveSupport() else builder

      // Create SparkSession
      val spark = builder.getOrCreate()

      // Set log level to "DEBUG"
      if (Try(sys.env("DEBUG_SPARK").toBoolean).getOrElse(false)) spark.sparkContext.setLogLevel("DEBUG")

      // Register UDTs
      UDTRegistration.register(userDefinedTypes)

      if (useSedona) {
        GeoJsonUDT.register()
        SedonaContext.create(spark)
      } else {
        spark
      }
    }

  }

}
