package eu.`2msoftware`.glue

import akka.actor.typed.ActorSystem
import com.`2m_software`.Extraction
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import java.math.BigInteger

object Glue extends App {

  trait GlueAction
  case class StartExtraction(source: Storage, target: Storage) extends GlueAction

  trait Storage
  case class CSV_Storage(config: String) extends Storage

  def Glue(extractionID: Long = 0): Behavior[GlueAction] = Behaviors.setup { context =>
    //setup
    val extraction = context.spawn(Extraction.Extraction(), s"Extraction_$extractionID")
    Glue(extractionID + 1)

    //message handling
    Behaviors.receiveMessage { message =>
      message match {
        case StartExtraction(source, target) =>
          extraction ! Extraction.Start()
          Behaviors.same
      }
    }
  }

  val ExtractionSystem = ActorSystem(Glue(), "GLUE")
  ExtractionSystem ! StartExtraction(source = CSV_Storage("csv_storage1"), target = CSV_Storage("csv_storage2"))
  // Thread.sleep(1000)
  // ExtractionSystem.terminate()
}
