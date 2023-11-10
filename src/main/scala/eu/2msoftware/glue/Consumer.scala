package eu.`2msoftware`.glue

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import eu.`2msoftware`.glue.Extraction._
import eu.`2msoftware`.glue.storages.csv.ConsumerCSV._
import eu.`2msoftware`.glue.storages.csv.CSV_Storage

case class ConsumerFactory() {

  def get_behavior(storageObject: Glue.StorageObject): Behavior[ExtAction] =
    storageObject.storage match {
      case _: CSV_Storage =>
        ConsumerCSV(storageObject, null)
      case _ =>
        null
    }

}
