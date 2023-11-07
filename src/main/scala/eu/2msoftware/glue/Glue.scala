package eu.`2msoftware`.glue

import akka.actor.typed.ActorSystem
import com.`2m_software`.Extraction._
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import java.math.BigInteger
import akka.japi.pf.FI.Apply

object Glue extends App {

  trait GlueAction
  case class StartExtraction(source: StorageObject, target: StorageObject, package_size: Int) extends GlueAction

  trait Storage
  case class CSV_Storage(config: String) extends Storage{
  }
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
    source = StorageObject("source1.csv", CSV_Storage("csv_storage1")),
    target = StorageObject("target1.csv", CSV_Storage("csv_storage2")),
    package_size = 500
  )

  //extraction 2
  ExtractionSystem ! StartExtraction(
    source = StorageObject("source2.csv", CSV_Storage("csv_storage1")),
    target = StorageObject("target2.csv", CSV_Storage("csv_storage2")),
    package_size = 500
  )

  // Thread.sleep(1000)
  // ExtractionSystem.terminate()
}
