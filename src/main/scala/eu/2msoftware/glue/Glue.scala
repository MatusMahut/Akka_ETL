package eu.`2msoftware`.glue

import akka.actor.typed.ActorSystem
import eu.`2msoftware`.glue.storages.csv.CSV_Storage
import eu.`2msoftware`.glue.Extraction._
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import java.math.BigInteger

object Glue extends App {

  trait GlueAction
  case class StartExtraction(source: StorageObject, target: StorageObject, package_size: Int) extends GlueAction
  case class StorageObject(objectName: String, storage: Storage)

  def Glue(extractionID: Long = 0): Behavior[GlueAction] = Behaviors.setup { context =>
    // setup
    Behaviors.same

    // message handling
    Behaviors.receiveMessage { message =>
      message match {
        case StartExtraction(source, target, package_size) =>
          val extraction = context.spawn(Extraction(source, target, package_size), s"Extraction_$extractionID")         
          extraction ! Start()
          Glue(extractionID + 1)
      }
    }
  }

  val ExtractionSystem = ActorSystem(Glue(), "GLUE")
  //extraction 1
  ExtractionSystem ! StartExtraction(
    source = StorageObject("source1.csv", new CSV_Storage("csv1")),
    target = StorageObject("target1.csv", new CSV_Storage("csv2")),
    package_size = 500
  )

  //extraction 2
  ExtractionSystem ! StartExtraction(
    source = StorageObject("source2.csv", new CSV_Storage("csv1")),
    target = StorageObject("target2.csv", new CSV_Storage("csv1")),
    package_size = 500
  )

  // Thread.sleep(1000)
  // ExtractionSystem.terminate()
}
