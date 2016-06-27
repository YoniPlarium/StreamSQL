package com.plarium.stream

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.hadoop.conf.Configuration


object Utils {

  def convetColTsToDatetime(fromTs: Column): Column = {
    from_unixtime(fromTs / 1000, "yyyy-MM-dd HH:mm:ss.SSS")
  }

  def filterJsonPerEvent(line: String, conds: List[String]): Boolean = {
    val jsRow = parse(line)
    val JString(event_name) = jsRow \ "event_name"
    conds.contains(event_name.values.toString)
  }


  def PMfilterJsonPerEvent(line: String): Boolean = {
    val jsRow = parse(line)
    val JInt(product_action_id) = jsRow \ "product_action_id"
    product_action_id.equals(3)
  }

  def PopulateDimsMap(sqlContext: SQLContext, sc: SparkContext, BD: Map[String, Broadcast[Seq[String]]], DimsNeeded: List[String], DimsKey: Map[String, String], inDF: DataFrame): DataFrame = {

    val listDimsNeeded = DimsNeeded.toSet.toList
    var tempDF: DataFrame = inDF

    val newDF = BD.map(dim => {
      val splitmapKey = dim._1.split(',')
      val dimName = splitmapKey(0)

      if (listDimsNeeded.contains(dimName)) {
        import sqlContext.implicits._
        val dimStruct = new StructType(Array(StructField("ref_" + dimName + "_id", StringType, nullable = true), StructField(dimName + "_name", StringType, nullable = true)))

        val paralell = sc.parallelize(dim._2.value)

        val dimRDD = paralell.map(r => {
          val line = r.toString.split('|')
          try {
            Row(line(splitmapKey(1).toInt), line(splitmapKey(2).toInt))
          } catch {
            case e: Exception => {
              Row("", "")
            }
          }
        })

        val dimDF = broadcast(sqlContext.createDataFrame(dimRDD, dimStruct))

        tempDF = tempDF.join(broadcast(dimDF), tempDF(DimsKey(dimName)) === dimDF("ref_" + dimName + "_id"), "left_outer").drop("ref_" + dimName + "id")
      }

      tempDF
    })

    tempDF
  }

  //val lst = HistoryFilesFromHDFS("hdfs://plh000211.localdomain:8020","/data/SigmaData/archive/bundle_vikings/",15227,15231)
  /*###################################################################################################333333 */
  def HistoryFilesFromHDFS(HdfsNN: String, ArchFolder: String, fromArchive: Int, toArchive: Int): List[String] = {
    var dirList: List[String] = List()
    val conf = new Configuration()
    conf.set("fs.defaultFS", HdfsNN)
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)

    for (archNum <- fromArchive to toArchive) {

      val exists = fs.exists(new org.apache.hadoop.fs.Path(ArchFolder + archNum.toString + "/"))
      if (exists == true) {
        val chkFolder = List(HdfsNN + ArchFolder + archNum.toString + "/*")
        driverLog.log.debug("adding " + chkFolder.mkString(",") + " to List")
        dirList = dirList ::: chkFolder
      }
    }

    driverLog.log.debug("Returning List to SC: " + dirList.mkString(","))
    dirList
  }


}

/*###################################################################################################333333 */

object SQLContextSingleton extends Serializable {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

/*###################################################################################################333333 */

object DimsSingleton extends Serializable {
  @volatile private var MapDimsinstance: Map[String, Broadcast[Seq[String]]] = null

  def getDimsList(sc: SparkContext): Map[String, Broadcast[Seq[String]]] = {
    if (MapDimsinstance == null) {
      synchronized {
        if (MapDimsinstance == null) {
          driverLog.log.debug("Dims Start")
          MapDimsinstance = Map(
            "cluster,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_cluster.csv").collect()),
            "network,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_network.csv").collect()),
            "country,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_country.csv").collect()),
            "device,1,5" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_device.csv").collect()),
            "quest,1,3" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_quest.csv").collect()),
            "progress_type,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_progress_type.csv").collect()),
            "progress_status,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_progress_status.csv").collect()),
            "product,1,2" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_product.csv").collect()),
            "player_action,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_player_action.csv").collect()),
            "battle_type,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_battle_type.csv").collect()),
            "product_action,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_product_action.csv").collect()),
            "movement_source_type,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_movement_source_type.csv").collect()),
            "transaction_source,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_transaction_source.csv").collect()),
            "payment_method,0,1" -> sc.broadcast(sc.textFile("/user/spark/dims/dim_payment_method.csv").collect())
          )

          driverLog.log.debug("Dims End")
        }
      }
    }
    MapDimsinstance
  }
}

/*###################################################################################################333333 */

object currSingleton extends Serializable {
  @volatile private var currSeq: DataFrame = _

  def getCurr(sqlContext: SQLContext, isRefresh: Int): DataFrame = {

    val currSt = StructType(Array(StructField("curr_day", DataTypes.IntegerType, true)
      , StructField("curr", DataTypes.StringType, true)
      , StructField("us_val", DataTypes.DoubleType, true)
    ))

    var currRows: RDD[Row] = null

    if (isRefresh == 1) {
      currRows = sqlContext.sparkContext.textFile("hdfs://plh000211.localdomain:8020/data/SigmaData/dim/dim_currency_daily_rate/dim_currency_daily_rate.txt")
        .map(row => {
        val line = row.split('|')
        try {
          Row(line(0).toInt, line(1), line(3).toDouble)
        } catch {
          case e: Exception => Row(0, "", 0)
        }
      })
    }
    else {
      currRows = sqlContext.sparkContext.parallelize(Seq("0||0"))
        .map(row => {
        val line = row.split('|')
        try {
          Row(line(0).toInt, line(1), line(3).toDouble)
        } catch {
          case e: Exception => Row(0, "", 0)
        }
      })
    }

    currSeq = broadcast(sqlContext.createDataFrame(currRows, currSt))

    currSeq
  }
}

/*###################################################################################################333333 */


object apiCurrency extends Serializable {
  @volatile private var currSeq: DataFrame = _
  @volatile private var lastUpdateTime: Long = 0L

  def getCurr(sqlContext: SQLContext): DataFrame = {

    var currRows: RDD[Row] = null
    var tempDF: DataFrame = null

    val currSt = StructType(Array(StructField("curr", DataTypes.StringType, true), StructField("us_val", DataTypes.DoubleType, true)))

    try {

      val timeInterval = System.currentTimeMillis - lastUpdateTime

      // update every 12H (1000*60*720)
      if (currSeq == null || timeInterval > 43200000) {
        driverLog.log.debug("Currency - update rates")

        val url = "https://openexchangerates.org/api/latest.json?app_id=ac5da23f52a74efc84a0c3974b1990ee"
        val Jsonresult = scala.io.Source.fromURL(url).mkString

        val jsRow = parse(Jsonresult)
        val JObject(curlist) = jsRow \ "rates"

        implicit val formats = DefaultFormats

        val CurRDD = sqlContext.sparkContext.parallelize(
          curlist.map(r => {
            (r._1, r._2.extract[Double])
          }).toSeq)

        currRows = CurRDD.map(r => {
          Row(r._1, r._2)
        })

        tempDF = sqlContext.createDataFrame(currRows, currSt)
        lastUpdateTime = System.currentTimeMillis
      }
      else {
        driverLog.log.debug("Currency - Using last currSeq")
        tempDF = currSeq
      }
    }
    catch {
      case e: Exception => {
        driverLog.log.warn("Currency rate fecth error: " + e.getMessage)
        if (currSeq == null) {
          currRows = sqlContext.sparkContext.parallelize(Seq(Row("none", 0)))
          tempDF = broadcast(sqlContext.createDataFrame(currRows, currSt))
        }
        else {
          driverLog.log.debug("Currency - Using last currSeq")
          tempDF = currSeq
        }
      }
    }

    currSeq = broadcast(tempDF)
    currSeq
  }
}

/*###################################################################################################333333 */



