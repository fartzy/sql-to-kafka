

def rollUpDateListMap(dateList: Map[String, List[Int]]
                     , rollUp: Map[String, List[String]]) : Map[String, List[Int]] = {

  rollUp
    .map { case (k, v) =>
      (k,
        v.map(t =>
          dateList.getOrElse(t, List.empty[Int]))
          .filter(_.size >= 1)
          .transpose
          .map( l => l.reduce(_ + _))
          .map(n => if (n > 0) 1 else 0)
      )}
}

val map1: Map[String, List[Int]] = Map("ios" -> List(0,0,1,1,1,0,1)
  ,"android" -> List(1,0,1,1,1,0,1)
  ,"mac" -> List(0,0,1,0,0,1,1) )

val map2: Map[String, List[String]] = Map("mobile" -> List("ios", "android")
  ,"all" -> List("ios", "android", "mac"))


//   map2
//    .map { case (k, v) =>
//      (k,
//      v.map(t =>
//        map1.getOrElse(t, List.empty[Int]))
//          .filter(_.size >= 1)
//          .transpose
//          .map( l => l.reduce(_ + _))
//          .map(n => if (n > 0) 1 else 0)
//      )}
//
//        .transpose }

rollUpDateListMap(map1, map2)


val historyMap = Map((1,1) -> (78999787, 3),
    (1,2) -> (78999790, 3),
    (1,3) -> (78999793, 100),
    (2,1) -> (78999711, 10),
    (2,2) -> (78999721, 10),
    (2,3) -> (78999731, 10),
    (2,4) -> (78999741, 0),
    (1,4) -> (78999893, 0),
    (3,1) -> (78999741, 0))

val historyMap2 = Map((1,1) -> (78999787, 3, 1, false),
  (1,2) -> (78999790, 3, 1, false),
  (1,3) -> (78999793, 100, 0, false),
  (2,1) -> (78999711, 10, 13, false),
  (2,2) -> (78999721, 10, 13, true ),
  (2,3) -> (78999731, 10, 10, true),
  (2,4) -> (78999741, 0, 0,false),
  (1,4) -> (78999893, 0, 0, true),
  (3,1) -> (78999741, 0, 13, true))



val inRow = (3,2, 78999743)

val qq  = (inRow._1, inRow._2, historyMap.get((inRow._1,inRow._2 - 1)).get._1, inRow._3 - historyMap.get((inRow._1,inRow._2 - 1)).get._1)


import scala.collection.mutable
import scala.util.Try
def addUserAcitictyToMapAndAggregate(historyMap: Map[(Int, Int), (Int, Int)]
                                    , inRow: (Int, Int, Int)
                                    , aggregateSet: Map[Int,(Int, Int)]) = {
  val maxStep = aggregateSet.size

  val lastStepDuration = if (inRow._2 > 1 ) {
    inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1
  } else 0

    //historyMap.get((inRow._1, maxStep)).get._1

  val mapPlusDurationRow = historyMap ++ Map((inRow._1, inRow._2 - 1) ->
    (historyMap.get((inRow._1, inRow._2 - 1)).get._1, lastStepDuration))

  val mapPlusIncomingRow = mapPlusDurationRow ++ Map((inRow._1, inRow._2) -> (inRow._3, 0))

  val newAggregateSetRow: Map[Int,(Int, Int)] = {
      if (inRow._2 > 1 ) {
        Map(inRow._2 - 1 -> (aggregateSet.get(inRow._2 - 1).get._1 + lastStepDuration, aggregateSet.get(inRow._2 - 1).get._1 + 1))
    } else  Map(maxStep -> (0, aggregateSet.get(maxStep).get._1 + 1 ))
  }

  val mapWithDurations = mapPlusIncomingRow
    .toList
    .map { case (k, v) => (k._2, v._2) }


  val getDistinctSteps = mapWithDurations.map { case (f, _) => f }.toSet

  getDistinctSteps.map(f =>
    (f, mapWithDurations.filter(_._1 == f)
      .map { case (k, v) => v }.sum /
      mapWithDurations.filter { case (k1, _) => k1 == f }.size))
}






//def addUserActivityToMapAndAggregateV1(historyMap: Map[(Int, Int), (Int, Int)]
//                                     , inRow: (Int, Int, Int)
//                                     , aggregateMap: Map[Int,(Int, Int)]) = {
//
//  val maxStep = aggregateMap.size
//
//  val lastStepDuration = if (inRow._2 > 1 ) {
//    inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1
//  } else 0
//
//  val historyMapPlusDurationRow = {
//    if (inRow._2 > 1 ) {
//      historyMap ++ Map((inRow._1, inRow._2 - 1) ->
//        (historyMap.get((inRow._1, inRow._2 - 1)).get._1, lastStepDuration))
//    } else historyMap ++ Map((inRow._1, inRow._2 - 1) ->
//      (historyMap.get((inRow._1, maxStep)).get._1, 0))
//  }
//
//  val newAggregateMapRow: Map[Int,(Int, Int)] = {
//    if (inRow._2 > 1 ) {
//      Map(inRow._2 - 1 -> (aggregateMap.get(inRow._2 - 1).getOrElse(0,0)._1 + lastStepDuration, aggregateMap.get(inRow._2 - 1).getOrElse(0,0)._1 + 1))
//    } else  Map(maxStep -> (0, aggregateMap.get(maxStep).get._1 + 1 ))
//  }
//
//  (aggregateMap ++ newAggregateMapRow)
//    .mapValues { case (v1, v2) => v1 / v2}
//}





/*
Purpose: To accept an an event row during the photo upload process, the event row being three things...
            1.session_id  2.step_id  3.timestamp
         To return an aggregated result set that has average amount of time taken per step across all sessions.
         The aggregated result set will be in form of...
            1.key = step_id, value = (totTime, totCount, aveTime)

Asymptotic Complexity:  The average case of the function is constant time O(1), and the worst case is a has table
          resize O(1).

Functional Paradigm:  The function is not side effecting and does not use mutable variables, so it will fair well
         in a concurrent system, and it will be easier to test because it is referentially transparent.

Summary: This function will take 4 parameters, and will return a tuple with two return sets.
         The `inRow` parameter will update the two incoming maps to create the two output maps,
         `outHistoryMap` is the updated, historic set of records and `outAggregateMap` is the aggregated set.
         As a new record comes in, the previous record gets updated with the duration.
Parameters:
  inHistoryMap: Map[(Int, Int), (Int, Int)] - the historic map of records, before being updated
  inRow: (Int, Int, Int) - the incoming row, the 1st field is session_id, 2nd is step_id, 3rd is timestamp
  inAggregateMap: Map[Int, aggMapValue] - the aggregateMap of records, BEFORE being updated
  maxStep: Int - the maximum amount of steps will be known, and it was "4" in the example on the whiteboard

Returns:
  Tuple2(
       outAggregateMap: Map[Int, aggMapValue] /* Description - the old aggregateMap of records, AFTER being updated */
     , outHistoryMap: Map[(Int, Int), (Int, Int)] /* Description - the historic map of records, AFTER being updated */
    )

Logic Breakdown Per Return Value:
    -outAggregateMap logic
      -mainly updates the previous inRow step, using the current inRow step, and there are three cases
        - 1. first record ever - 'step 1' row will be inserted, only time a straight insert happens
        - 2. second time 'step 1' record - will insert a 'step 4' and count + 1, keeping the count of 'step 4's
        - 3. all other cases - updates with duration that is obtained from history map and re-aggregates

    -outHistoryMap logic
      -Also mainly updates the previous inRow, using current inRow, and there are two cases
        - 1. Any 'step 1' inRow - 'step 1' row will be inserted, nothing else
        - 2. All other cases - row will be inserted and then updates the duration of the previous inRow by subtracting
             previous timestamp from current timestamp

Limitation: No calculation for time taken for 'step 4's, because there is no indicator given yet that a
            'step 4' finished
 */

case class aggMapVal(val totTime: BigInt, val totCount: BigInt, val aveTime: Double)
def addRowAndAggregate(inHistoryMap: Map[(Int, Int), (Int, Int)]
                       , inRow: (Int, Int, Int)
                       , inAggregateMap: Map[Int, aggMapVal]
                       , maxStep: Int) = {
  val historyMap = inHistoryMap ++ Map((inRow._1, inRow._2) -> (inRow._3, 0))

  if ((inRow._2 == 1) && !inAggregateMap.contains(inRow._2)) {
    // first ever record for aggregateMap

    (inAggregateMap ++ Map(inRow._2 -> aggMapVal(0, 0, 0)), historyMap)

  } else if ((inRow._2 == 1) && inAggregateMap.contains(inRow._2)) {
    //1st step, but not the first record with 1st step ever

    val newAggregateMapRow = Map(maxStep -> aggMapVal(0, inAggregateMap.getOrElse(maxStep, aggMapVal(0, 0, 0)).totCount + 1, 0))
    ((inAggregateMap ++ newAggregateMapRow)
      .mapValues { case aggMapVal(v1, v2, v3) => aggMapVal(v1, v2, v1.toDouble / v2.toDouble) }
    ,historyMap)
  } else {
    //common case - No check for failure of previous step because there is guaranteed to be
    // a previous step since we are always inserting a previous step first

      val prevStepTime = inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1
      val newAggregateMapRow = { Map(inRow._2 - 1 ->
        aggMapVal(inAggregateMap.get(inRow._2 - 1).get.totTime + prevStepTime
          , inAggregateMap.get(inRow._2 - 1).get.totCount + 1
          , 0))}
     ((inAggregateMap ++ newAggregateMapRow)
        .mapValues { case aggMapVal(v1, v2, v3) => aggMapVal(v1, v2, v1.toDouble / v2.toDouble)}
      , historyMap ++ Map((inRow._1, inRow._2 - 1) ->
        (historyMap.get((inRow._1, inRow._2 - 1)).get._1, prevStepTime)))}
}


def addRowAndAggregateV2(inHistoryMap: Map[(Int, Int), (Int, Int)]
                       , inRow: (Int, Int, Int)
                       , aggregateMap: Map[Int, aggMapVal]
                       , maxStep: Int) = {


  val historyMap = inHistoryMap ++ Map((inRow._1, inRow._2) -> (inRow._3, 0))

  ( if (inRow._2 > 1 ) {
    val prevStepTime = inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1
    val newAggregateMapRow = { Map(inRow._2 - 1 ->
      aggMapVal(aggregateMap.get(inRow._2 - 1).getOrElse(aggMapVal(0,0,0)).totTime + prevStepTime
        , aggregateMap.get(inRow._2 - 1).getOrElse(aggMapVal(0,0,0)).totCount + 1
        , 0))}
    (aggregateMap ++ newAggregateMapRow)
      .mapValues { case aggMapVal(v1, v2, v3) => aggMapVal(v1, v2, v1.toDouble / v2.toDouble)}
  } //if the 1st step comes in, and it is the very first record
   else if (aggregateMap.contains(inRow._2)){
      val newAggregateMapRow = Map(maxStep ->
      aggMapVal(0, aggregateMap.get(maxStep).getOrElse(aggMapVal(0,0,0)).totCount + 1, 0))
    (aggregateMap ++ newAggregateMapRow)
      .mapValues { case aggMapVal(v1, v2, v3) => aggMapVal(v1, v2, v1.toDouble / v2.toDouble)}
  } else {
    aggregateMap ++ Map(inRow._2 -> aggMapVal(0,0,0))
  },
    if (inRow._2 > 1 ) {
      val prevStepTime = inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1
      historyMap ++ Map((inRow._1, inRow._2 - 1) ->
        (historyMap.get((inRow._1, inRow._2 - 1)).get._1, prevStepTime))
    }  else if (historyMap.contains(inRow._1, maxStep)) {
      historyMap ++ Map((inRow._1, maxStep) -> (historyMap.get((inRow._1, maxStep)).getOrElse(0,0)._1, 0))
    } else {
      historyMap
    }
  )

}



//def processRow()
val mutableMap = new mutable.HashMap[(Int,Int), (Int, Int)]()

//Test Data for addRowAndAggregate
val inRow1 = (3,1, 78999743)
val inRow2 = (3,2, 78999745)
val inRow3 = (3,3, 78999749)
val inRow4 = (3,4, 78999759)
val inRow5 = (4,1, 78999769)
val inRow6 = (4,2, 78999879)
val inRow7 = (4,3, 78999883)
val inRow8 = (4,4, 78999983)
val inRow9 = (5,1, 78999987)
val processed1 = addRowAndAggregate(Map.empty[(Int, Int), (Int, Int)], inRow1, Map.empty[Int,aggMapVal], 4)
val processed2 = addRowAndAggregate(processed1._2, inRow2, processed1._1, 4)
val processed3 = addRowAndAggregate(processed2._2, inRow3, processed2._1, 4)
val processed4 = addRowAndAggregate(processed3._2, inRow4, processed3._1, 4)
val processed5 = addRowAndAggregate(processed4._2, inRow5, processed4._1, 4)
val processed6 = addRowAndAggregate(processed5._2, inRow6, processed5._1, 4)
val processed7 = addRowAndAggregate(processed6._2, inRow7, processed6._1, 4)
val processed8 = addRowAndAggregate(processed7._2, inRow8, processed7._1, 4)
val processed9 = addRowAndAggregate(processed8._2, inRow9, processed8._1, 4)

mutableMap+=(1,2) -> (78999790, 3)


val mutableHistoryMap = Map((1,1) -> (78999787, 3),
  (1,2) -> (78999790, 3),
  (1,3) -> (78999793, 100),
  (2,1) -> (78999711, 10),
  (2,2) -> (78999721, 10),
  (2,3) -> (78999731, 10),
  (2,4) -> (78999741, 0),
  (1,4) -> (78999893, 0),
  (3,1) -> (78999741, 0))


addRowAndAggregate(historyMap, inRow, Map.empty[Int, aggMapVal], 4)

//addUserAcitictyToMapAndAggregate(historyMap, inRow)


//def addUserAcitictyToMapAndAggregateV2(historyMap2: Map[(Int, Int), (Int, Int, Int, Boolean)]
//                                     , inRow: (Int, Int, Int)) = {
//
//  def recurseThroughMaphistoryMap(historyMap: Map[(Int, Int), (Int, Int)]
//                                 , inRow: (Int, Int, Int)
//                                 , updatedFlag: Boolean) = historyMap match {
//    case historyMap if (updatedFlag) => historyMap
//    case h::t => if ()
//  }
//
//}

def addUserAcitictyToMapAndAggregateV2(historyMap2: Map[(Int, Int), (Int, Int)]
                                       , inRow: (Int, Int, Int)) = {

  def editHistoryMap(historyMap: Map[(Int, Int), (Int, Int, Int, Boolean)]
                                  , inRow: (Int, Int, Int)
                                  , updatedFlag: Boolean) = {
    val maxStep =  historyMap
      .toList
      .map{ case ((_,s),(_,_,_,_)) =>
        s }
      .toSet
      .max

    val durationLastUserStep: Option[Int]= {
      if (inRow._2 > 1 ) {
        Some(inRow._3 - historyMap.get((inRow._1, inRow._2 - 1)).get._1)
      } else {
        None
      }
    }

    historyMap ++ Map()


  }

}








