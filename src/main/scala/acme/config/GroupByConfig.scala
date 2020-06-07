package acme.config

import java.util

import com.fasterxml.jackson.annotation.JsonProperty
import com.typesafe.config.ConfigFactory
import _root_.scala.collection.JavaConverters._

class GroupByConfig {

  val APPLICATION_CONFIG_FILE = System.getenv("APPLICATION_CONFIG_FILE")
  val config = ConfigFactory.load(APPLICATION_CONFIG_FILE)
  val groupByStep = config.getConfigList("groupBySteps").get(0)
  val inputTopic = groupByStep.getString("topic.in")
  val outputTopic = groupByStep.getString("topic.out")
  val groupByColumnList = groupByStep.getStringList("groupBy.columns").asScala.toList
  val aggregateColumnList = groupByStep.getStringList("aggregation.columns").asScala.toList
  val aggregateOperationList = groupByStep.getStringList("aggregation.operations").asScala.toList
  val dateFormatList = groupByStep.getStringList(" aggregation.dateformat").asScala.toList
  val aliasList = groupByStep.getStringList("aggregation.outputcolumns").asScala.toList

}
