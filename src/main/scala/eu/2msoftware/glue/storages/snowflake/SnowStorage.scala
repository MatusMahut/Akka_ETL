package eu.`2msoftware`.glue.storages.snowflake
import eu.`2msoftware`.glue.Storage

class SnowStorage(config: String) extends Storage(s"src/main/storages/snowflake/$config.conf") {

    val acc = settings.get("account")
    val user = settings.get("user")
    val pass = settings.get("password")
    val db = settings.get("database")
    val schema = settings.get("schema")
    val warehouse = settings.get("warehouse")
  
}
