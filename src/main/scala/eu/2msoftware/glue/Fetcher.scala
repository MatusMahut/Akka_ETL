package eu.`2msoftware`.glue

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.Behavior
import com.`2m_software`.Extraction._
import scala.io.Source
import scala.reflect.ClassTag

trait Fetcher {

  def fetch(filename: String, line: Int, package_size: Int, extraction: ActorRef[ExtAction]): Behavior[ExtAction]
  def close(): Behavior[ExtAction]

  def ImutableFetcherCSV(filename: String, line: Int = 0, package_size: Int = 10, extraction: ActorRef[ExtAction]): Behavior[ExtAction] =
    Behaviors.setup { context =>
      Behaviors.same

      Behaviors.receiveMessage { message =>
        message match {
          case FetchPackage() =>
            fetch(filename, line, package_size, extraction)
          case Close() =>
            close()
        }
      }
    }

}
