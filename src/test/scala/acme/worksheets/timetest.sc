import java.lang.System

import scala.collection.immutable.NumericRange


def time[R](block: => R): R = {
  val t0 = System.nanoTime()
  val result = block    // call-by-name
  val t1 = System.nanoTime()
  println("Elapsed time: " + ((t1 - t0) / 1000000d) + "milli seconds")
  result
}

//Wrap all method calls for get the proper time reading, for example change this...
val result1 = 1 to 1000 sum

// ... into this
val result2 = time { 1 to 1000 sum }

def createLongList(amount: Long):List[Long] = {

  def addToL(a: Long, c: Long, acc: List[Long]): List[Long] = (a,c)  match {
    case (a,c) if (c == 0) => acc
    case (a,c) => addToL(a,c - 1, acc :+ c )
  }

  addToL(amount, amount, List.empty[Long])
}
//val r3 = time { 1d to 10000000000d sum }

//createLongList(1000000000L)
