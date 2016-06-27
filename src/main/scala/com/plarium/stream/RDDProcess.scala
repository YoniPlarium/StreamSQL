package com.plarium.stream

import org.apache.spark.streaming.dstream.DStream
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{TaskContext, SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.weekofyear
import org.apache.spark.sql.functions.year
import java.lang.{Long => JLong}
import scala.collection.JavaConversions._
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.broadcast

trait Event {
  def BaseSelList: List[String]

  def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit
}

object Event extends Serializable {


  /*######################################################################################################################################*/

  private class sessionsRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.connScmBase ::: SchemaFactory.loginScmBase

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      val dimSel = List("country_name", "device_name")
      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel ::: dimSel)
      val techCols = List("id", "date_part")

      val connSel = SchemaFactory.getSelCols(SchemaFactory.connScmBase ::: SchemaFactory.baseSel ::: techCols ::: List("country_name"))
      val loginSel = SchemaFactory.getSelCols(SchemaFactory.loginScmBase ::: SchemaFactory.baseSel ::: techCols ::: List("device_name"))

      execLog.log.debug("Start sessionsRDD")

      val mainSess = DF.select(SelectCols.head, SelectCols.tail: _*).filter($"event_name" === "CONNECT" || $"event_name" === "LOGIN" || $"event_name" === "DISCONNECT")
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"session_id"))
        .cache()


      execLog.log.debug("sessionsRDD => CONNECT")
      mainSess.select(connSel.head, connSel.tail: _*).filter($"event_name" === "CONNECT")
        .withColumn("connect_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .saveToEs(indexName, EsMap)

      execLog.log.debug("sessionsRDD => LOGIN")
      mainSess.select(loginSel.head, loginSel.tail: _*).filter($"event_name" === "LOGIN")
        .withColumn("login_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .saveToEs(indexName, EsMap)


      execLog.log.debug("sessionsRDD => DISCONNECT")
      mainSess.filter($"event_name" === "DISCONNECT")
        .withColumn("disconnect_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .select($"id", $"disconnect_datetime", $"date_part", $"event_name")
        .saveToEs(indexName, EsMap)

      mainSess.unpersist()

      execLog.log.debug("End sessionsRDD")

    }
  }

  /*######################################################################################################################################*/

  private class depositsRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.depositScm ::: SchemaFactory.sendDepositScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      var currDF: DataFrame = null

      execLog.log.debug("Start depositsRDD")

      currDF = apiCurrency.getCurr(sqlContext)

      /*

      try {
        currDF = currSingleton.getCurr(sqlContext,1)
      }catch {
        case e: Exception => {
          println(e.toString)
          currDF = currSingleton.getCurr(sqlContext,0)
        }
      }

      */

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel ::: List("payment_method_name", "transaction_source_name"))

      execLog.log.debug(" depositsRDD => Main")

      val preJoinMainDepositDF = DF.select(SelectCols.head, SelectCols.tail: _*).filter($"event_name" === "TRANSACTION_RESULT" || $"event_name" === "SEND_TRANSACTION")
        .withColumn("id", concat($"player_id", lit("_"), $"internal_transaction_id"))
        .withColumn("date_part", date_format($"event_datetime", "yyyy-MM"))
        .withColumn("day", date_format($"event_datetime", "yyyyMMdd"))

      val mainDepositDF = preJoinMainDepositDF
        .join(broadcast(currDF), preJoinMainDepositDF("deposit_currency") === currDF("curr"), "left_outer")
        .drop("curr").drop("day")
        .withColumn("usd_deposit", $"deposit_amount" / $"us_val")
        .drop("us_val")
        .na.fill(-1, Seq("transaction_status_id"))
        .cache()

      execLog.log.debug(" depositsRDD => SEND_TRANSACTION")

      mainDepositDF.filter($"event_name" === "SEND_TRANSACTION").saveToEs(indexName, EsMap)

      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT")

      val resultDF = mainDepositDF.where($"event_name" === "TRANSACTION_RESULT").cache()


      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT => 1")
      resultDF.filter($"transaction_status_id".equalTo(1) || $"transaction_status_id".equalTo(-1))
        .withColumn("Approved_status_time", $"event_datetime")
        .drop("original_offer_amount")
        .drop("payment_method_id")
        .drop("bonus_amount_1")
        .drop("bonus_amount_2")
        .drop("receive_amount_1")
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT => 2")

      resultDF
        .withColumn("refused_status_time", $"event_datetime")
        .select("player_id", "id", "date_part", "refused_status_time", "event_datetime", "event_name", "transaction_status_id", "session_id", "network_name", "internal_transaction_id", "network_id", "cluster_id", "cluster_name")
        .where($"transaction_status_id" === 2)
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT => 3")
      resultDF
        .withColumn("canceled_status_time", $"event_datetime")
        .select("player_id", "id", "date_part", "canceled_status_time", "event_datetime", "event_name", "transaction_status_id", "session_id", "network_name", "internal_transaction_id", "network_id", "cluster_id", "cluster_name")
        .where($"transaction_status_id" === 3)
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT => 4")
      resultDF
        .withColumn("pending_status_time", $"event_datetime")
        .select("player_id", "id", "date_part", "pending_status_time", "event_datetime", "event_name", "transaction_status_id", "session_id", "network_name", "internal_transaction_id", "network_id", "cluster_id", "cluster_name")
        .where($"transaction_status_id" === 4)
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" depositsRDD => TRANSACTION_RESULT => 5")
      resultDF
        .withColumn("error_status_time", $"event_datetime")
        .select("player_id", "id", "date_part", "error_status_time", "event_datetime", "event_name", "transaction_status_id", "session_id", "network_name", "internal_transaction_id", "network_id", "cluster_id", "cluster_name")
        .where($"transaction_status_id" === 5)
        .saveToEs(indexName, EsMap)

      mainDepositDF.unpersist()
      preJoinMainDepositDF.unpersist()
      resultDF.unpersist()
      currDF = null

      execLog.log.debug("End depositsRDD")
    }
  }

  /*######################################################################################################################################*/

  private class triggerRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.triggerScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel ::: List("player_action_name", "product_name"))

      execLog.log.debug("Start triggerRDD")

      DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "DEPOSIT_TRIGGER")
        .withColumn("date_part", date_format($"event_datetime", "yyyy-MM"))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"session_id", lit("_"), $"internal_transaction_id"))
        .drop("event_ts").drop("event_name")
        .saveToEs(indexName, EsMap)

      execLog.log.debug("End triggerRDD")
    }
  }


  /*######################################################################################################################################*/

  private class userGameProfileRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.gameProfileScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import org.elasticsearch.spark.rdd.EsSpark

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: List("event_datetime"))
      val tempSelList = List("cluster_id", "player_id", "event_ts", "event_name", "event_datetime")

      execLog.log.debug("Start userGameProfileRDD")

      import sqlContext.implicits._
      val ColRDD = DF.select(SelectCols.head, SelectCols.tail: _*).filter($"event_name" === "USER_GAME_PROFILE")
        .map(row => {

        val ScmList = row.schema.map(f => {
          f.name + "," + f.dataType.toString
        }).toList
        val Rowkey = row.getAs("player_id").toString + "_" + row.getAs("cluster_id").toString

        val ColWithData = for {(x, i) <- ScmList.zipWithIndex
                               if (!row.isNullAt(i)); if (!tempSelList.contains(x.split(",")(0)))
        } yield x.split(",")(0)

        var tempMap: Map[String, String] = Map()
        tempMap += "id" -> Rowkey
        tempMap += "event_datetime" -> row.getAs("event_datetime").toString
        tempMap += "player_id" -> row.getAs("player_id").toString
        tempMap += "event_name" -> row.getAs("event_name").toString
        tempMap += "cluster_id" -> row.getAs("cluster_id").toString


        for (col <- ColWithData) {
          tempMap += col -> row.getAs(col).toString
        }

        tempMap

      })

      EsSpark.saveToEs(ColRDD, indexName, EsMap)

      execLog.log.debug("End userGameProfileRDD")
    }
  }


  /*######################################################################################################################################*/

  private class profileRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.userProfileScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel)

      val UserProfileSelect = SchemaFactory.getSelCols(SchemaFactory.userProfileScm ::: SchemaFactory.baseSel ::: List("id"))

      execLog.log.debug("Start profileRDD")

      import sqlContext.implicits._

      val mainDF = DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "USER_PROFILE")
        .cache()

      FindLast(sqlContext, mainDF, "USER_PROFILE", UserProfileSelect, EsMap, indexName)

      mainDF.unpersist()

      execLog.log.debug("End profileRDD")
    }

    def FindLast(sqc: SQLContext, df: DataFrame, EventFilter: String, SelectList: List[String], esM: Map[String, String], indexName: String): Unit = {
      import sqc.implicits._

      val maxUserProfileDF = df.filter($"event_name" === EventFilter)
        .groupBy($"player_id", $"cluster_id").agg(max($"event_ts").alias("max_ts"))
        .withColumnRenamed("player_id", "min_player_id")
        .withColumnRenamed("cluster_id", "min_cluster_id")

      df.filter($"event_name" === EventFilter)
        .join(maxUserProfileDF, $"player_id" === $"min_player_id" && $"cluster_id" === $"min_cluster_id" && $"event_ts" === $"max_ts", "inner")
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id"))
        .select(SelectList.head, SelectList.tail: _*)
        .saveToEs(indexName, esM)

      maxUserProfileDF.unpersist()

    }
  }

  /*######################################################################################################################################*/

  private class regsRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.regScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel)

      execLog.log.debug("Start regsRDD")

      import sqlContext.implicits._

      DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "REGISTRATION")
        .withColumn("reg_datetime", Utils.convetColTsToDatetime($"registration_date"))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id"))
        .drop("event_ts").drop("registration_date")
        .saveToEs(indexName, EsMap)

      execLog.log.debug("End regsRDD")
    }
  }

  /*######################################################################################################################################*/

  private class set_levelRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.LevelScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel)

      execLog.log.debug("Start set_levelRDD")

      import sqlContext.implicits._

      val setLevelBaseDF = DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "SET_LEVEL")
        .filter($"level_type_id" === 1 || $"level_type_id" === 2)
        .cache()

      FindLast(sqlContext, setLevelBaseDF, 1, "main_level", EsMap, indexName)
      FindLast(sqlContext, setLevelBaseDF, 2, "Secondary_level", EsMap, indexName)

      setLevelBaseDF.unpersist()

      execLog.log.debug("End set_levelRDD")
    }

    def FindLast(sqc: SQLContext, df: DataFrame, EventFilter: Int, renameCol: String, esM: Map[String, String], indexName: String): Unit = {

      import sqc.implicits._

      val SelCol: Column = new Column(renameCol)

      val maxLevelDF = df.filter($"level_type_id" === EventFilter)
        .groupBy($"player_id", $"cluster_id").agg(max($"event_ts").alias("max_ts"))
        .withColumnRenamed("player_id", "min_player_id")
        .withColumnRenamed("cluster_id", "min_cluster_id")

      df.filter($"level_type_id" === EventFilter)
        .join(maxLevelDF, $"player_id" === $"min_player_id"
        && $"cluster_id" === $"min_cluster_id"
        && $"event_ts" === $"max_ts", "inner")
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id"))
        .withColumnRenamed("level_id", renameCol)
        .select(SelCol, $"id", $"event_name")
        .saveToEs(indexName, esM)

      maxLevelDF.unpersist()

    }

  }

  /*######################################################################################################################################*/

  private class resourcesRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.resourcesScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {


      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel)
      val fullSelCols = SchemaFactory.getSelCols(SelectCols ::: List("id", "event_datetime"))

      execLog.log.debug("Start resourcesRDD")

      import sqlContext.implicits._

      val mainDF = DF.select(SelectCols.head, SelectCols.tail: _*).filter($"event_name" === "DISCONNECT").cache()

      val maxResourcesDF = mainDF.groupBy($"player_id", $"cluster_id").agg(max($"event_ts").alias("max_ts"))
        .withColumnRenamed("player_id", "min_player_id")
        .withColumnRenamed("cluster_id", "min_cluster_id")

      mainDF.join(maxResourcesDF, $"player_id" === $"min_player_id"
        && $"cluster_id" === $"min_cluster_id"
        && $"event_ts" === $"max_ts", "inner")
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id"))
        .select(fullSelCols.head, fullSelCols.tail: _*)
        .drop("event_ts").drop("event_name")
        .saveToEs(indexName, EsMap)


      maxResourcesDF.unpersist()
      mainDF.unpersist()

      execLog.log.debug("End resourcesRDD")

    }

  }


  /*######################################################################################################################################*/

  private class prodMoveRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.prodMoveScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel ::: List("movement_source_type_name", "product_action_name"))

      execLog.log.debug("Start prodMoveRDD")

      DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "PRODUCT_MOVEMENTS")
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"event_ts", lit("_"), $"event_order"))
        .drop("event_name").drop("event_order").drop("event_ts")
        .saveToEs(indexName, EsMap)

      execLog.log.debug("End prodMoveRDD")

    }
  }


  /*######################################################################################################################################*/

  private class battleRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.battleScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      val SelectCols = SchemaFactory.getSelCols(BaseSelList ::: SchemaFactory.baseSel ::: List("battle_type_name"))

      execLog.log.debug("Start battleRDD")

      DF.select(SelectCols.head, SelectCols.tail: _*)
        .filter($"event_name" === "BATTLE")
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"session_id", lit("_"), $"battle_id"))
        .drop("event_name").drop("event_ts")
        .saveToEs(indexName, EsMap)

      execLog.log.debug("End battleRDD")
    }
  }


  /*######################################################################################################################################*/

  private class progressRDD extends Event with Serializable {

    override def BaseSelList = SchemaFactory.openScm ::: SchemaFactory.startScm ::: SchemaFactory.finishScm

    override def CalcAndSaveToEs(DF: DataFrame, EsMap: Map[String, String], sqlContext: SQLContext, indexName: String): Unit = {

      import sqlContext.implicits._

      val baseSel = SchemaFactory.baseSel ::: List("progress_type_name", "progress_status_name", "quest_name")
      val openSel = SchemaFactory.getSelCols(SchemaFactory.openScm ::: baseSel)
      val startSel = SchemaFactory.getSelCols(SchemaFactory.startScm ::: baseSel)
      val finishSel = SchemaFactory.getSelCols(SchemaFactory.finishScm ::: baseSel)

      execLog.log.debug("Start progressRDD")


      execLog.log.debug(" progressRDD => 2")
      DF.select(openSel.head, openSel.tail: _*).filter($"event_name" === "PROGRESS_START" && $"progress_type" === "2")
        .withColumn("open_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"keyid"))
        .drop("event_ts")
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" progressRDD => 3")
      DF.select(startSel.head, startSel.tail: _*)
        .filter($"event_name" === "PROGRESS_START" && $"progress_type" === "3")
        .withColumn("start_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"keyid"))
        .drop("event_ts")
        .saveToEs(indexName, EsMap)

      execLog.log.debug(" progressRDD => PROGRESS_FINISH")
      DF.select(finishSel.head, finishSel.tail: _*)
        .filter($"event_name" === "PROGRESS_FINISH")
        .withColumn("finish_datetime", Utils.convetColTsToDatetime($"event_ts"))
        .withColumn("date_part", concat(year($"event_datetime"), lit("_"), weekofyear($"event_datetime")))
        .withColumn("id", concat($"player_id", lit("_"), $"cluster_id", lit("_"), $"keyid"))
        .select("id", "date_part", "finish_datetime", "event_name", "event_datetime", "progress_type_name")
        .saveToEs(indexName, EsMap)

      execLog.log.debug("End progressRDD")
    }


  }


  /* APPLY FUNCTION ################################################################################################################################# */

  def apply(s: String): Event = s match {
    case "sessions" => new sessionsRDD
    case "deposits" => new depositsRDD
    case "trigger" => new triggerRDD
    case "userGameProfile" => new userGameProfileRDD
    case "profile" => new profileRDD
    case "regs" => new regsRDD
    case "set_level" => new set_levelRDD
    case "resources" => new resourcesRDD
    case "prodMove" => new prodMoveRDD
    case "battle" => new battleRDD
    case "progress" => new progressRDD
  }

}