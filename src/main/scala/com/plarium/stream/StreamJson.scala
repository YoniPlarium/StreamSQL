package com.plarium.stream

import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger, PropertyConfigurator, LogManager, Layout}
import java.lang.{Long => JLong}

object driverLog extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Driver")
}

object execLog extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("Exec")
}

object rootLogger extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getRootLogger
}

object StreamJson extends Serializable {

  def createContext(appFileName: String, EventsCombination: String, confMap: Map[String, Settings])
  : StreamingContext = {

    driverLog.log.debug("Creating new context")

    val confSetting = confMap("app")
    val generalConf = confMap("general")

    val sparkConf = confSetting.getSparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(confSetting.secsInterval))

    driverLog.log.debug("Checkpointing at Start")
    ssc.checkpoint(confSetting.checkpointDirectory)

    val EventsArgs = EventsCombination.split(",")
    val EventsFilterList = generalConf.getEventsFilterList(EventsArgs)

    val UpdateMap = generalConf.getMapFromConf("Stream.ESUpdateConf")
    val InsertMap = Map()

    val eventDimsMap = generalConf.getMapFromConf("Stream.EventsDims")
    val dimKeyMap = generalConf.getMapFromConf("Stream.DimKey")
    val indexMap = generalConf.getMapWithReplace("Stream.EventsIndex", "XXXGAMEXXX", confSetting.game)

    val topicsSet = confSetting.topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> confSetting.brokers)

    val KafkaMsg = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    KafkaMsg.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      var CountofMsg = 0L
      var LogMsg = "["
      offsetRanges.foreach(off => {
        CountofMsg = CountofMsg + (off.untilOffset - off.fromOffset)
        val countRng: Long = off.untilOffset - off.fromOffset
        LogMsg = LogMsg + "(" + off.topic + "," + off.partition.toString + "," + off.fromOffset.toString + "->" + off.untilOffset.toString + "," + countRng.toString + ")"
      })
      LogMsg = "Total: " + CountofMsg.toString + " , " + LogMsg + "]"
      driverLog.log.debug(LogMsg)

      //offsetRanges.foreach( driverLog.log.debug(_) )
    }
    //.repartition(confSetting.repartNum)
    val messages = KafkaMsg.repartition(confSetting.repartNum).map(_._2)
      .filter(row => {
      Utils.filterJsonPerEvent(row, EventsFilterList)
    })

    driverLog.log.debug("Start for eachRDD")

    messages.foreachRDD { (rdd, time) =>

      driverLog.log.debug("Start Batch: " + time)
      rdd.cache()

      if (rdd.toLocalIterator.nonEmpty) {

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        val DimBroadcast = DimsSingleton.getDimsList(rdd.sparkContext)

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

        val BaseDF = sqlContext.read.schema(SchemaFactory.makeSchemafromList(UniqueBaseSelectList)).json(rdd)

        val BaseDFWithDims = Utils.PopulateDimsMap(sqlContext, rdd.sparkContext, DimBroadcast, dimsNeeded, dimKeyMap, BaseDF)
          .withColumn("event_datetime", Utils.convetColTsToDatetime($"event_ts"))
        BaseDFWithDims.cache()
        driverLog.log.debug("Count of batch:" + BaseDFWithDims.count())

        EventsArgs.foreach(
          argEvent => {
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
          }
        )

        BaseDF.unpersist()
        BaseDFWithDims.unpersist()
        sqlContext.clearCache()
      }


      rdd.unpersist()
      driverLog.log.debug("End Batch: " + time)
    }

    driverLog.log.debug("End for eachRDD")

    ssc

  }

  /* ######################################################################################################################################### */

  def main(args: Array[String]): Unit = {

    rootLogger.log.setLevel(Level.WARN)


    val Array(appFileName, eventsRunCombo) = args
    val config = new Settings(appFileName)
    val generalConf = new Settings("general.conf")

    driverLog.log.debug("START APPLICATION: " + config.appGroup)

    val ssc = StreamingContext.getOrCreate(config.checkpointDirectory,
      () => {
        createContext(appFileName, eventsRunCombo, Map("app" -> config, "general" -> generalConf))
      })

    sys.ShutdownHookThread {
      driverLog.log.debug(config.appGroup + " , Gracefully stopping Spark Streaming Application")
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      driverLog.log.debug(config.appGroup + " , Application stopped")
    }

    driverLog.log.debug(config.appGroup + " START SSC")
    ssc.start()
    ssc.awaitTermination()
  }
}
