package eu.`2msoftware`.glue.storages.CSV

import java.io.FileWriter
import akka.actor.typed.scaladsl.Behaviors
import com.`2m_software`.Extraction._
import java.io.File
import eu.`2msoftware`.glue.Glue._

object ConsumerCSV {
  def ConsumerCSV(storageObject: StorageObject, filewriter: FileWriter = null): Behaviors.Receive[ExtAction] = Behaviors.receive { (context, message) =>
    message match {
      case Open() =>
        ConsumerCSV(storageObject, new FileWriter(new File(storageObject.objectName), true))
      case ConsumePackage(data) =>
        try
          filewriter.write(new String(data))
        catch {
          case e: Exception =>
            filewriter.close()
        }
        context.log.info(s"CONSUMER -> Appended package to file ${storageObject.objectName} .")
        Behaviors.same
      case Commit() =>
        filewriter.close()
        context.log.info(s"CONSUMER -> COMMITED.")
        Behaviors.same
    }
  }
}
