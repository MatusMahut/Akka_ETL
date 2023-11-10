package eu.`2msoftware`.glue.storages.csv
import eu.`2msoftware`.glue.FetcherFactory
import akka.actor.typed.{ ActorRef, Behavior }
import eu.`2msoftware`.glue.Extraction._
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import scala.io.Source
import scala.reflect.ClassTag
import akka.actor.typed.scaladsl.ActorContext
import eu.`2msoftware`.glue.Glue

object FetcherCSV {

  def FetcherCSV(storageObject: Glue.StorageObject, line: Int = 0, package_size: Int = 10, extraction: ActorRef[ExtAction]): Behavior[ExtAction] =
    Behaviors.setup { context =>
      Behaviors.same

      Behaviors.receiveMessage { message =>
        message match {
          case FetchPackage() =>
            fetch(context, storageObject, line, package_size, extraction)
          case Close() =>
            close(context)
        }
      }
    }

  def fetch(
      context: ActorContext[ExtAction],
      storObj: Glue.StorageObject,
      line: Int,
      pack_size: Int,
      extraction: ActorRef[ExtAction]
  ): Behavior[ExtAction] = {
    val full_path = storObj.storage.asInstanceOf[CSV_Storage].path + storObj.objectName
    val data      = readCSVLines(full_path, line, line + pack_size)
    if (data.isEmpty) {
      context.log.info(s"FETCHER -> Fetching Finished")
      extraction ! FetchingFinished()
      Behaviors.stopped
    } else {
      context.log.info(s"FETCHER -> Taken $pack_size lines from line $line")
      extraction ! Fetched(data)
      FetcherCSV(storObj, line + pack_size, pack_size, extraction) // new behaviour
    }
  }

  def close(context: ActorContext[ExtAction]): Behavior[ExtAction] = {
    context.log.info(s"Fetcher closed")
    Behaviors.stopped
  }

  def readCSVLines(fileName: String, fromLine: Int, toLine: Int): Array[Byte] = {
    val lineIT = Source.fromFile(fileName).getLines()
    val lines  = lineIT.toArray(ClassTag[String](classOf[String])).slice(fromLine, toLine)
    addLineToByteArray(Array.emptyByteArray, lines, 0)
  }

  def addLineToByteArray(data: Array[Byte], lines: Array[String], line: Int): Array[Byte] =
    if (line == lines.length) data
    else
      addLineToByteArray(Array.concat(data, lines(line).getBytes(), "\n".getBytes()), lines, line + 1)
}
