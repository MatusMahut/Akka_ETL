package eu.`2msoftware`.glue

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import com.`2m_software`.Extraction._
import eu.`2msoftware`.glue.storages.CSV.ConsumerCSV._

case class Consumer() {

  def get_behavior(storageObject: Glue.StorageObject): Behavior[ExtAction] =
    storageObject.storage match {
      case Glue.CSV_Storage(config) =>
        ConsumerCSV(storageObject, null)
      case _ =>
        null
    }

}
