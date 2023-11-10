package eu.`2msoftware`.glue

import scala.collection.mutable.HashMap
import java.io.FileWriter
import java.io.File
import java.io.Reader
import java.io.FileReader
import java.io.BufferedReader
import java.util.LinkedList
import scala.annotation.tailrec

abstract class Storage(config: String) {
  
  val br       = new BufferedReader(new FileReader(config))
  val settings = readParams(br)

  def readParams(br: BufferedReader): HashMap[String, String] = {
    @tailrec
    def readParamsTR(br: BufferedReader, settings: HashMap[String, String] = new HashMap()): HashMap[String, String] = {
      val line = br.readLine()
      if (line != null) {
        val splitLine = line.split(": ")
        settings.addOne(splitLine(0), splitLine(1))
        readParamsTR(br, settings)
      } else {
        settings
      }
    }
    readParamsTR(br)
  }
}
