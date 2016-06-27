package com.plarium.stream

import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf

class Settings(AppFileName: String) extends Serializable {

  val config = ConfigFactory.parseResources(AppFileName)

  def AppGroup = try {
    config.getString("Stream.AppConf.AppGroup")
  } catch {
    case e: Exception => ""
  }

  val topics = try {
    config.getString("Stream.AppConf.topics")
  } catch {
    case e: Exception => ""
  }
  val brokers = try {
    config.getString("Stream.AppConf.brokers")
  } catch {
    case e: Exception => ""
  }
  val checkpointDirectory = try {
    config.getString("Stream.AppConf.checkpointDirectory")
  } catch {
    case e: Exception => ""
  }
  val appGroup = try {
    config.getString("Stream.AppConf.appGroup")
  } catch {
    case e: Exception => ""
  }
  val secsInterval = try {
    config.getInt("Stream.AppConf.secsInterval")
  } catch {
    case e: Exception => -1
  }
  val repartNum = try {
    config.getInt("Stream.AppConf.repartNum")
  } catch {
    case e: Exception => -1
  }
  val game = try {
    config.getString("Stream.AppConf.game")
  } catch {
    case e: Exception => ""
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()

    config.withOnlyPath("Stream.Spark").entrySet() map (line => {
      driverLog.log.debug(removeRootKey(2, line.getKey) + " , value is: " + line.getValue.unwrapped().toString)
      sparkConf.set(removeRootKey(2, line.getKey), line.getValue.unwrapped().toString)
    })

    sparkConf
  }

  def getMapFromConf(confPath: String): Map[String, String] = {
    config.withOnlyPath(confPath).entrySet().map {
      es => (removeRootKey(2, es.getKey), es.getValue.unwrapped().toString)
    }.toMap
  }

  def getMapWithReplace(confPath: String, StringToReplace: String, NewString: String): Map[String, String] = {
    getMapFromConf(confPath).map(
      kv => {
        (kv._1 -> kv._2.replaceAll(StringToReplace, NewString))
      })
  }


  def getEventsFilterList(EventsList: Array[String]): List[String] = {
    var tempList: List[String] = Nil
    EventsList.map { e => {
      config.getString("Stream.EventsFilter." + e.toString).split(",").foreach({ x => tempList ::= x})
    }
    }

    tempList
  }

  private def removeRootKey(numPos: Int, theKey: String): String = {
    val splitArr = theKey.split("\\.")
    val back = splitArr.takeRight(splitArr.length - numPos)
    back.mkString(".")
  }


}
