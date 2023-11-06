package com.`2m_software`
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import scala.io.Source
import scala.reflect.ClassTag
import java.io.FileWriter
import java.io.File

object Extraction {

  trait Action
  case class FetchPackage()                    extends Action
  case class Fetched(data: Array[Byte])        extends Action
  case class FetchingFinished()                extends Action
  case class ConsumePackage(data: Array[Byte]) extends Action
  case class Commit()                          extends Action
  case class Close()                           extends Action
  case class Open()                            extends Action
  case class Terminate()                       extends Action
  case class Start()                           extends Action

  // val tempFilePrefix = "TMP_"

  def Extraction(): Behavior[Action] = Behaviors.setup { context =>
    val imutableConsumerCSV = context.spawn(ImutableConsumerCSV("target.csv"), "imutableConsumerCSV")
    val imutableFetcherCSV =
      context.spawn(ImutableFetcherCSV(filename = "source.csv", 0, 1000, context.self), "imutableFetcherCSV")

    Behaviors.receiveMessage { message =>
      message match {
        case Start() =>
          imutableConsumerCSV ! Open()
          imutableFetcherCSV ! FetchPackage()
          Behaviors.same
        case Terminate() =>
          Behaviors.stopped
        case FetchingFinished() =>
          imutableConsumerCSV ! Commit()
          Behaviors.same
        case Fetched(data: Array[Byte]) =>
          imutableConsumerCSV ! ConsumePackage(data)
          imutableFetcherCSV ! FetchPackage()
          Behaviors.same
        case _ =>
          context.log.info(s"Unknown message")
          Behaviors.same
      }

    }
  }

  def ImutableFetcherCSV(filename: String, line: Int = 0, package_size: Int = 10, extraction: ActorRef[Action]): Behavior[Action] =
    Behaviors.receive { (context, message) =>
      message match {
        // val ExtractionSystem = ActorSystem(ImutableFetcherCSV("filename.csv",0,10,ImutableConsumerCSV("filename.csv")), "emotionActorSystem")
        case FetchPackage() =>
          val data = readCSVLines(filename, line, line + package_size)
          if (data.isEmpty) {
            context.log.info(s"FETCHER -> Fetching Finished")
            extraction ! FetchingFinished()
            Behaviors.stopped
          } else {
            context.log.info(s"FETCHER -> Taken $package_size lines from line $line")
            extraction ! Fetched(data)
            ImutableFetcherCSV(filename, line + package_size, package_size, extraction) // new behaviour
          }

        case Close() =>
          context.log.info(s"Fetcher closed")
          Behaviors.stopped
      }

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

  def ImutableConsumerCSV(filename: String, filewriter: FileWriter = null): Behaviors.Receive[Action] = Behaviors.receive {
    (context, message) =>
      message match {
        case Open() =>
          ImutableConsumerCSV(filename, new FileWriter(new File(filename), true))
        case ConsumePackage(data) =>
          try
            filewriter.write(new String(data))
          catch {
            case e: Exception =>
              filewriter.close()
          }
          context.log.info(s"CONSUMER -> Appended package to file $filename .")
          Behaviors.same
        case Commit() =>
          filewriter.close()
          context.log.info(s"CONSUMER -> COMMITED.")
          Behaviors.same
      }

  }
}
