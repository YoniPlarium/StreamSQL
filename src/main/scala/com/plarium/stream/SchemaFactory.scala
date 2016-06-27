package com.plarium.stream

import org.apache.spark.sql.types.{StructField, DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._


object SchemaFactory extends Serializable {


  def makeSchemafromList(scmList: List[String]): StructType = {

    val schema =
      StructType(scmList.map(ScmLine => {
        val LineArr = ScmLine.split(',')
        val fieldName = LineArr(0)
        val ScmType = LineArr(1) match {
          case "StringType" => DataTypes.StringType
          case "LongType" => DataTypes.LongType
          case "IntegerType" => DataTypes.IntegerType
          case "DoubleType" => DataTypes.DoubleType
        }

        StructField(fieldName, ScmType, true)
      }
      ))

    schema
  }


  def getSelCols(scmList: List[String]): List[String] = {

    val UniqueList = scmList.toSet.toList
    val retList = UniqueList.map(line => {
      line.split(",")(0)
    }).toList

    retList
  }

  val baseSel = List("network_name", "cluster_name", "event_datetime")

  val SchemaBase = List(
    "cluster_id,StringType"
    , "player_id,StringType"
    , "event_ts,LongType"
    , "session_id,StringType"
    , "event_name,StringType"
    , "network_id,IntegerType"
  )

  val connScmBase = SchemaBase ::: List(
    "login_country_id,StringType"
  )

  val loginScmBase = SchemaBase ::: List(
    "device_desc,StringType"
    , "device_id,StringType"
    , "os_version,StringType",
    "linked_player_id,StringType",
    "timezone,StringType",
    "game_version,StringType"
  )

  val depositScm = SchemaBase ::: List(
    "internal_transaction_id,StringType"
    , "offer_id,StringType"
    , "deposit_amount,DoubleType"
    , "deposit_currency,StringType"
    , "transaction_source_id,IntegerType"
    , "external_transaction_id,StringType"
    , "transaction_status_id,IntegerType"
    , "bonus_amount_1,IntegerType"
    , "bonus_amount_2,IntegerType"
    , "receive_amount_1,IntegerType"
  )

  val sendDepositScm = List(
    "original_offer_amount,DoubleType",
    "payment_method_id,IntegerType"
  )

  val triggerScm = List(
    "cluster_id,StringType"
    , "player_id,StringType"
    , "event_ts,LongType"
    , "session_id,StringType"
    , "event_name,StringType"
    , "network_id,IntegerType"
    , "internal_transaction_id,StringType"
    , "item_id,StringType"
    , "item_name,StringType"
    , "action_id,IntegerType"
    , "trigger_group_type,StringType"
    , "trigger_group_type_id,StringType"
    , "trigger_id,StringType"
    , "trigger_type,StringType"
  )

  val openScm = List(
    "cluster_id,StringType"
    , "player_id,StringType"
    , "event_ts,LongType"
    , "session_id,StringType"
    , "event_name,StringType"
    , "network_id,IntegerType"
    , "keyid,StringType"
    , "category_type,StringType"
    , "category_id,StringType"
    , "sub_category_id,StringType"
    , "sub_category_type,StringType"
    , "keyname,StringType"
    , "progress_type,StringType"
    , "start_rate,StringType"
  )

  val startScm = List(
    "cluster_id,StringType"
    , "player_id,StringType"
    , "event_ts,LongType"
    , "session_id,StringType"
    , "event_name,StringType"
    , "network_id,IntegerType"
    , "progress_type,StringType"
    , "keyid,StringType"
    , "category_type,StringType"
    , "category_id,StringType"
    , "sub_category_id,StringType"
    , "sub_category_type,StringType"
    , "keyname,StringType"
  )

  val finishScm = List(
    "cluster_id,StringType"
    , "player_id,StringType"
    , "event_ts,LongType"
    , "session_id,StringType"
    , "event_name,StringType"
    , "keyid,StringType"
    , "state,IntegerType"
    , "finish_rate,StringType"
    , "progress_type,StringType"
  )

  val userProfileScm = List(
    "event_name,StringType",
    "event_ts,LongType",
    "cluster_id,StringType",
    "network_id,IntegerType",
    "player_id,StringType",
    "social_id,StringType",
    "attribute1,StringType",
    "attribute2,StringType",
    "attribute3,StringType",
    "birthday,LongType",
    "email,StringType",
    "first_name,StringType",
    "last_name,StringType",
    "gender,StringType",
    "ingame_friends,IntegerType",
    "language,StringType",
    "num_friends,IntegerType",
    "url_picture,StringType"
  )

  val gameProfileScm = List(
    "event_name,StringType",
    "event_ts,LongType",
    "cluster_id,StringType",
    "player_id,StringType",
    "base_name,StringType",
    "game_language_id,StringType",
    "game_world_id,IntegerType",
    "is_test_player,IntegerType",
    "is_vip,IntegerType",
    "location_x,IntegerType",
    "location_y,IntegerType",
    "user_name,StringType"
  )

  val LevelScm = List(
    "event_name,StringType",
    "network_id,IntegerType",
    "event_ts,LongType",
    "cluster_id,StringType",
    "player_id,StringType",
    "level_type_id,IntegerType",
    "new_level_ind,IntegerType",
    "level_id,IntegerType"
  )

  val regScm = List(
    "event_name,StringType",
    "network_id,IntegerType",
    "event_ts,LongType",
    "registration_date,LongType",
    "cluster_id,StringType",
    "player_id,StringType",
    "login_source_id,StringType",
    "login_source_type_id,StringType"
  )

  val resourcesScm = List(
    "event_name,StringType",
    "event_ts,LongType",
    "cluster_id,StringType",
    "network_id,IntegerType",
    "player_id,StringType",
    "resource1_id,StringType",
    "resource10_id,StringType",
    "resource2_id,StringType",
    "resource3_id,StringType",
    "resource4_id,StringType",
    "resource5_id,StringType",
    "resource6_id,StringType",
    "resource7_id,StringType",
    "resource8_id,StringType",
    "resource9_id,StringType",
    "resource_amount_1,IntegerType",
    "resource_amount_10,DoubleType",
    "resource_amount_2,IntegerType",
    "resource_amount_3,IntegerType",
    "resource_amount_4,IntegerType",
    "resource_amount_5,IntegerType",
    "resource_amount_6,IntegerType",
    "resource_amount_7,IntegerType",
    "resource_amount_8,IntegerType",
    "resource_amount_9,DoubleType"
  )


  val prodMoveScm = List(
    "event_name,StringType",
    "event_ts,LongType",
    "event_order,IntegerType",
    "cluster_id,StringType",
    "network_id,IntegerType",
    "player_id,StringType",
    "product_action_id,IntegerType",
    "product_id,StringType",
    "product_name,StringType",
    "product_count,IntegerType",
    "bonus_amount,IntegerType",
    "bonus_product_id,IntegerType",
    "movement_source_id,StringType",
    "movement_source_type_id,StringType",
    "package_id,StringType",
    "player_target_id,StringType",
    "payment_product_id_1,StringType",
    "payment_product_id_2,StringType",
    "payment_product_id_3,StringType",
    "payment_product_id_4,StringType",
    "payment_product_id_5,StringType",
    "payment_product_id_6,StringType",
    "payment_product_id_7,StringType",
    "payment_product_id_8,StringType",
    "payment_product_name_1,StringType",
    "payment_product_name_2,StringType",
    "payment_product_name_3,StringType",
    "payment_product_name_4,StringType",
    "payment_product_name_5,StringType",
    "payment_product_name_6,StringType",
    "payment_product_name_7,StringType",
    "payment_product_name_8,StringType",
    "payment_amount_1,IntegerType",
    "payment_amount_2,IntegerType",
    "payment_amount_3,IntegerType",
    "payment_amount_4,IntegerType",
    "payment_amount_5,IntegerType",
    "payment_amount_6,IntegerType",
    "payment_amount_7,IntegerType",
    "payment_amount_8,IntegerType"
  )

  val battleScm = List(
    "event_name,StringType",
    "event_ts,LongType",
    "cluster_id,StringType",
    "network_id,IntegerType",
    "player_id,StringType",
    "session_id,StringType",
    "attacker_ind,IntegerType",
    "battle_id,StringType",
    "battle_source_id,StringType",
    "battle_type_id,IntegerType",
    "base_id,StringType",
    "base_level,IntegerType",
    "base_status_id,IntegerType",
    "base_type_id,IntegerType",
    "battle_win_ind,IntegerType",
    "actual_travel_time,IntegerType",
    "main_attacker_ind,IntegerType",
    "main_defender_ind,IntegerType",
    "opponent_id,StringType",
    "player_base_ind,IntegerType",
    "player_defence_power,IntegerType",
    "player_health_power,IntegerType",
    "player_offence_power,IntegerType",
    "is_offline_action,IntegerType"
  )


  val listAllColsToSet = SchemaBase ::: connScmBase ::: loginScmBase ::: depositScm ::: sendDepositScm ::: triggerScm ::: openScm ::: startScm ::: finishScm ::: userProfileScm ::: gameProfileScm ::: LevelScm ::: regScm ::: resourcesScm ::: prodMoveScm ::: battleScm
  val setAllCols = listAllColsToSet.toSet
  val AllColsScm = setAllCols.toList

  /** **************************************** FOR SELECT ******************************************************************** */

}
