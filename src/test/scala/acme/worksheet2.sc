import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.lang.System
import java.time.{LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.immutable.NumericRange
import scala.util.{Failure, Success, Try}


//time { createLongList(80000L) }

def normalizeDate(format:String, dateStr: String): Option[String] = {

  val dateFormats = (format :: List("MMM dd uuuu hh:mm:ss:SSSa",
    "MMMM dd uuuu hh:mm:ss:SSSa",
    "MMM  d uuuu hh:mm:ss:SSSa",
    "MMMM  d uuuu hh:mm:ss:SSSa",
    "dd/MM/uuuu",
    "MMM dd, uuuu",
    "MMM d, uuuu",
    "MMMM dd, uuuu",
    "MMMM d, uuuu",
    "dd MMMM uuuu",
    "d MMMM uuuu",
    "dd MMM uuuu",
    "d MMM uuuu",
    "dd-MM-uuuu",
    "d-MM-uuuu",
    "uuuu-MM-dd hh:mm:ss",
    "yyyy-MM-dd HH:mm:ss"))
    .map(p => (p, DateTimeFormatter.ofPattern(p)))

  val trimmedDate = dateStr.trim
  if(trimmedDate.isEmpty)
    None
  else {
    val t = dateFormats.map {
      case (patternString, pattern) =>
        Try(LocalDateTime.parse(dateStr, pattern))
    }.find(_.isSuccess)

      t match {
        case Some(Success(g)) => Some(g.toEpochSecond(ZoneOffset.UTC).toString)
        case None => None
    }
  }
//  else {
//    dateFormats.toStream.map {
//      case (pattern, fmt) =>
//        Try(fmt.parse(trimmedDate))
//    }.find(_.isSuccess).map{ t =>
//      iso8601DateFormatter.format(t.get) }
//  }
}


//normalizeDate("yyyy-MM-dd HH:mm:ss","2018-01-01 10:30:00")
normalizeDate("yyyy-MM-dd HH:mm:ss","2018-01-01 10:30:00")
normalizeDate("yyyy-MM-dd HH:mm:ss","t")
val dft = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
val ldt = LocalDateTime.ofEpochSecond(1514802600L, 0, ZoneOffset.UTC)
dft.format(ldt)

val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
val datetime_format2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
val date_int_format = DateTimeFormatter.ofPattern("yyyyMMdd")
val last_extract_value="2018-05-09 10:04:25.375"
val time_value="2018-05-09 10:04:25"

val string_to_date = datetime_format.parse(last_extract_value)


//val epoch = string_to_date.getLong
//Date Back to string
date_int_format.format(string_to_date)

def stepThroughList(list: List[_]): String = list match {
  case (head::tail) => head.toString + stepThroughList(tail)
  case (head::tail) => head.toString + stepThroughList(tail)
}

val myList = List(1,2,3)

val opt = myList.headOption

//val date1 = dft.format(LocalDateTime.ofEpochSecond(deserialized, 0, ZoneOffset.UTC))

type InitType = Int

def deserialize(isVal: Boolean)(bytes: Array[Byte])(cbf: String => InitType ):  InitType = {
  val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
  val value = ois.readObject
  ois.close()
  if (isVal) cbf(value.asInstanceOf[String]) else value.asInstanceOf[InitType]
}



