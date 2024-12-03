package github.avinoamn.spark.process

import org.apache.spark.sql.types.UserDefinedType
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

trait SparkProcess extends extensions.SparkSessionType {
  def processName: String = getClass.getSimpleName.dropRight(1)

  def additionalConfigs: Map[String, Any] = Map()

  def extensions: Seq[SparkSessionExtensions => Unit] = Seq()

  def enableHiveSupport: Boolean = false

  def useSedona: Boolean = false

  def userDefinedTypes: Seq[UserDefinedType[_]] = Seq()

  final implicit val spark: SparkSession = SparkSession.build(
    processName,
    additionalConfigs,
    extensions,
    enableHiveSupport,
    useSedona,
    userDefinedTypes
  )
}
