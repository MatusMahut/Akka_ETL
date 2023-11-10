package eu.`2msoftware`.glue.storages.csv
import eu.`2msoftware`.glue.Storage

class CSV_Storage(config: String) extends Storage(s"src/main/storages/CSV/$config.conf") {

  val path = settings.get("path")
  
}
