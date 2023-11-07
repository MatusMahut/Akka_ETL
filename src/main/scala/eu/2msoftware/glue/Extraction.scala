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
import eu.`2msoftware`.glue.Glue
import eu.`2msoftware`.glue.Consumer
import eu.`2msoftware`.glue.Glue.StorageObject
import eu.`2msoftware`.glue.Fetcher

object Extraction {

  trait ExtAction
  case class FetchPackage()                    extends ExtAction
  case class Fetched(data: Array[Byte])        extends ExtAction
  case class FetchingFinished()                extends ExtAction
  case class ConsumePackage(data: Array[Byte]) extends ExtAction
  case class Commit()                          extends ExtAction
  case class Close()                           extends ExtAction
  case class Open()                            extends ExtAction
  case class Terminate()                       extends ExtAction
  case class Start()                           extends ExtAction

  // val tempFilePrefix = "TMP_"

  def Extraction(source: StorageObject, target: StorageObject, package_size: Int): Behavior[ExtAction] = Behaviors.setup { context =>
    val imutableConsumerCSV = context.spawn(Consumer().get_behavior(target), "Consumer")
    val imutableFetcherCSV  = context.spawn(Fetcher().get_behavior(source, package_size, context.self), "Fetcher")

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
}
