package com.glue
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorSystem
import akka.actor.typed.ActorRef
 
object Extraction{
 
    trait Action
    case class FetchPackage() extends Action
    case class Fetched(data: Array[Byte]) extends Action
    case class FetchingFinished() extends Action
    case class ConsumePackage(data: Array[Byte]) extends Action
    case class Commit() extends Action
    case class Close() extends Action
    case class Terminate() extends Action
    case class Start() extends Action

    def Extraction(): Behavior[Action] = Behaviors.setup { context => 
        val imutableConsumerCSV = context.spawn(ImutableConsumerCSV("target.csv"), "imutableConsumerCSV")
        val imutableFetcherCSV = context.spawn(ImutableFetcherCSV(filename = "source.csv", 0, 10), "imutableFetcherCSV")

        Behaviors.receiveMessage { message =>  
            message match {
                case Start() =>     
                    try {
                        fetch(imutableFetcherCSV)
                    } catch {
                        case _: Throwable => 
                            context.log.info(s"Fetcher closed ")
                            Behaviors.stopped    
                    }
                    Behaviors.same
                case Terminate() =>                              
                    Behaviors.stopped             
                case FetchingFinished() =>                        
                    //some action
                    Behaviors.same
                case Fetched(data: Array[Byte]) =>
                    //context.log.info(s"FETCHER -> Taken $package_size lines from line $line")
                    imutableConsumerCSV ! ConsumePackage(data)                   
                    Behaviors.same
                case _ => 
                    context.log.info(s"Unknown message")
                    Behaviors.same                
            }
            
        }
     }

    def fetch(imutableFetcherCSV: ActorRef[Action]): Unit = {
        countTo(0, 10000)
        try {
          imutableFetcherCSV ! FetchPackage()      
          fetch(imutableFetcherCSV)
        } catch {
            case _: Throwable => throw new Exception("Closed")
        }

    }

    def countTo(from: Int, to : Int): Int = {
        if ( from < to ) countTo(from + 1, to)
        to
    }
     

  def ImutableFetcherCSV(filename: String, line: Int = 0, package_size: Int = 10): Behavior[Action] = Behaviors.receive { ( context, message ) =>
    message match {
        //val ExtractionSystem = ActorSystem(ImutableFetcherCSV("filename.csv",0,10,ImutableConsumerCSV("filename.csv")), "emotionActorSystem")
        case FetchPackage() =>               
            //CSV PARSING...
            context.self ! Fetched(Array.emptyByteArray)
            if (line == 50) {
                context.self ! FetchingFinished()
                Behaviors.stopped
            } else{
            context.log.info(s"FETCHER -> Taken $package_size lines from line $line")
            ImutableFetcherCSV(filename, line + package_size,  package_size) // new behaviour
            }

        case Close() => 
            context.log.info(s"Fetcher closed")
            Behaviors.stopped
    }

  }

  def ImutableConsumerCSV( filename: String ): Behavior[Action] = Behaviors.receive { ( context, message ) =>
    message match {
        
        case ConsumePackage(Array.emptyByteArray) =>               
            //APPEND PACKAGE TO FILE
            context.log.info(s"CONSUMER -> Appended package to file $filename .")
            Behaviors.same
        case Commit() => 
            //CLOSE FILE
            context.log.info(s"CONSUMER -> File $filename closed.")
            Behaviors.same 
    }

  } 

}

object ExtractionTest extends App {
    val ExtractionSystem = ActorSystem(Extraction.Extraction(), "emotionActorSystem")
    ExtractionSystem ! Extraction.Start()
    Thread.sleep(1000)
    ExtractionSystem.terminate()
}

