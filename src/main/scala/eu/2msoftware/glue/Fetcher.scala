package eu.`2msoftware`.glue

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import com.`2m_software`.Extraction._
import scala.io.Source
import scala.reflect.ClassTag
import eu.`2msoftware`.glue.storages.CSV.FetcherCSV
import akka.actor.typed.scaladsl.ActorContext

case class Fetcher() {

  def get_behavior(storageObject: Glue.StorageObject, package_size: Int = 10, extraction: ActorRef[ExtAction]): Behavior[ExtAction] =
    storageObject.storage match {
      case Glue.CSV_Storage(config) =>
        FetcherCSV.FetcherCSV(storageObject, 0, package_size, extraction)
      case _ =>
        null
    }

}
