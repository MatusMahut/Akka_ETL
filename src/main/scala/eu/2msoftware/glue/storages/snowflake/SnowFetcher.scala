package eu.`2msoftware`.glue.storages.snowflake
import eu.`2msoftware`.glue.FetcherFactory
import akka.actor.typed.{ ActorRef, Behavior }
import eu.`2msoftware`.glue.Extraction._
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import scala.io.Source
import scala.reflect.ClassTag
import akka.actor.typed.scaladsl.ActorContext
import eu.`2msoftware`.glue.Glue

object SnowFetcher {

  def SnowFetcher(storageObject: Glue.StorageObject, package_size: Int = 10, extraction: ActorRef[ExtAction]): Behavior[ExtAction] =
    Behaviors.setup { context =>
      Behaviors.same

      Behaviors.receiveMessage { message =>
        message match {
          case FetchPackage() =>
            fetch(context, storageObject, package_size, extraction)
          case Close() =>
            close(context)
        }
      }
    }

  def fetch(
      context: ActorContext[ExtAction],
      storObj: Glue.StorageObject,
      pack_size: Int,
      extraction: ActorRef[ExtAction]
  ): Behavior[ExtAction] = {
    val data = Array.emptyByteArray
    if (data.isEmpty) {
      context.log.info(s"FETCHER -> Fetching Finished")
      extraction ! FetchingFinished()
      Behaviors.stopped
    } else {
      context.log.info(s"FETCHER -> Taken $pack_size lines")
      extraction ! Fetched(data)
      SnowFetcher(storObj,  pack_size, extraction) // new behaviour
    }
  }

  def close(context: ActorContext[ExtAction]): Behavior[ExtAction] = {
    context.log.info(s"Fetcher closed")
    Behaviors.stopped
  }

}
