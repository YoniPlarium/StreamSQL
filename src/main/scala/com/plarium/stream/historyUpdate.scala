package com.plarium.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.Partition
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.hadoop.conf.Configuration

object historyUpdate extends Serializable {

  def main(args: Array[String]): Unit = {

    rootLogger.log.setLevel(Level.WARN)

    val Array(appFileName, eventsRunCombo, hdfsNN, archFolder, fromArchive, toArchive, numPartHdfs) = args

    val confSetting = new Settings(appFileName)
    val generalConf = new Settings("general.conf")

    val sparkConf = confSetting.getSparkConf()

    val EventsArgs = eventsRunCombo.split(",")
    val EventsFilterList = generalConf.getEventsFilterList(EventsArgs)

    val UpdateMap = generalConf.getMapFromConf("Stream.ESUpdateConf")
    val InsertMap = Map()

    val eventDimsMap = generalConf.getMapFromConf("Stream.EventsDims")
    val dimKeyMap = generalConf.getMapFromConf("Stream.DimKey")
    val indexMap = generalConf.getMapWithReplace("Stream.EventsIndex", "XXXGAMEXXX", confSetting.game)

    driverLog.log.debug("START APPLICATION: " + confSetting.appGroup)
    driverLog.log.debug("Parameters : " + args.mkString("|"))

    val sc = new SparkContext(sparkConf)

    driverLog.log.debug("Read Files and Filter")

    val listOfFolders = Utils.HistoryFilesFromHDFS(hdfsNN, archFolder, fromArchive.toInt, toArchive.toInt)

    val historyFiles = sc.textFile(listOfFolders.mkString(","), numPartHdfs.toInt)
      .filter(row => {
      Utils.filterJsonPerEvent(row, EventsFilterList)
    })


    val sqlContext = SQLContextSingleton.getInstance(sc)

    driverLog.log.debug("Get Dims")
    val DimBroadcast = DimsSingleton.getDimsList(sc)

    import sqlContext.implicits._

    var BaseSelectList: List[String] = Nil
    var dimsNeeded: List[String] = Nil

    EventsArgs.foreach(
      argEvent => {
        val runObj = Event(argEvent)

        BaseSelectList = BaseSelectList ::: runObj.BaseSelList
        dimsNeeded = dimsNeeded ::: eventDimsMap(argEvent).split(",").toList
      })

    val UniqueBaseSelectList = BaseSelectList.toSet.toList

    driverLog.log.debug("Create Dataframe")
    val BaseDF = sqlContext.read.schema(SchemaFactory.makeSchemafromList(UniqueBaseSelectList)).json(historyFiles)
      .repartition(numPartHdfs.toInt)

    val BaseDFWithDims = Utils.PopulateDimsMap(sqlContext, sc, DimBroadcast, dimsNeeded, dimKeyMap, BaseDF)
      .withColumn("event_datetime", Utils.convetColTsToDatetime($"event_ts"))

    BaseDFWithDims.cache()
    driverLog.log.debug("Count of Batch: " + BaseDFWithDims.count())


    EventsArgs.foreach(
      argEvent => {
        driverLog.log.debug("START EVENT => " + argEvent)
        val runObj = Event(argEvent)
        runObj.CalcAndSaveToEs(BaseDFWithDims,
          if (generalConf.config.getString("Stream.EventsUpdateMethod." + argEvent) == "update") {
            UpdateMap
          } else {
            Map()
          },
          sqlContext,
          indexMap(argEvent)
        )
        driverLog.log.debug("END EVENT => " + argEvent)
      }
    )


    BaseDF.unpersist()
    BaseDFWithDims.unpersist()
    sqlContext.clearCache()

    driverLog.log.debug("FINISH")

  }

}
