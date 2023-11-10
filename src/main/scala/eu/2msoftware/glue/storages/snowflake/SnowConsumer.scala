package eu.`2msoftware`.glue.storages.snowflake

import akka.actor.typed.scaladsl.Behaviors
import eu.`2msoftware`.glue.Extraction._
import eu.`2msoftware`.glue.Glue._
import java.io.InputStream
import java.sql.DriverManager
import java.io.File
import java.io.FileInputStream
import java.sql.Connection
import net.snowflake.client.jdbc.SnowflakeConnection
import java.io.ByteArrayInputStream

object SnowConsumer {

  private val stagePrefix = "/AKKA_ETL/"
  private val userStage = "@~"

  def SnowConsumer(storageObject: StorageObject, packageNum: Int = 1): Behaviors.Receive[ExtAction] = Behaviors.receive { (context, message) =>
    message match {
      case Open() =>
        Behaviors.same
      case ConsumePackage(data) =>
        uploadStream(data, storageObject.asInstanceOf[SnowStorage], packageNum)
        context.log.info(s"CONSUMER -> Appended package to file ${storageObject.objectName} .")
        SnowConsumer(storageObject, packageNum)
      case Commit() =>
        commit()
        Behaviors.stopped
    }
  }

  def uploadStream(data: Array[Byte], stor: SnowStorage, packageNum: Int): Unit = {
    val filename = packageNum.toString()
    val url =
      s"jdbc:snowflake://${stor.acc}.snowflakecomputing.com?user=${stor.user}&password=${stor.pass}&db=${stor.db}&schema={$stor.schema}&warehouse=${stor.warehouse}"
    val prop                   = new java.util.Properties
    val connection: Connection = DriverManager.getConnection(url, prop)
    val inputStream            = new ByteArrayInputStream(data)
    // upload file stream to user stage
    connection.unwrap(classOf[SnowflakeConnection]).uploadStream(userStage, stagePrefix, inputStream, filename, true);
  }

  def commit(): Unit = {
    
  }

  def cleanup(): Unit = {

  }

}
