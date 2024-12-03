package github.avinoamn.spark.process.extensions

import org.apache.spark
import org.apache.spark.sql.types.UserDefinedType

private[process] trait UDTRegistration {
  implicit class UDTRegistrationExtensions(t: spark.sql.types.UDTRegistration.type) {
    def register(udt: UserDefinedType[_]): Unit = t.register(udt.userClass.getCanonicalName, udt.getClass.getCanonicalName)

    def register(udts: Seq[UserDefinedType[_]]): Unit = udts.foreach(register)
  }
}
