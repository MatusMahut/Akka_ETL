package eu.`2msoftware`.glue

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import eu.`2msoftware`.glue.Extraction._
import scala.io.Source
import scala.reflect.ClassTag
import eu.`2msoftware`.glue.storages.csv.FetcherCSV
import eu.`2msoftware`.glue.storages.csv.CSV_Storage
import akka.actor.typed.scaladsl.ActorContext

case class FetcherFactory() {

  def get_behavior(storageObject: Glue.StorageObject, package_size: Int = 10, extraction: ActorRef[ExtAction]): Behavior[ExtAction] =
    storageObject.storage match {
      case _: CSV_Storage =>
        FetcherCSV.FetcherCSV(storageObject, 0, package_size, extraction)
      case _ =>
        null
    }

}
