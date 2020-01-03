package com.github.mrpowers.my.cool.project
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object jsonReader extends App{
  val spark = SparkSession.builder().master( master = "local").getOrCreate()
  val filename = args(0)
  val sc = spark.sparkContext
  //val dataFile = sc.textFile("/home/admin/Downloads/winemag-data-130k-v2.json1")
  val dataFile = sc.textFile(filename)
  case class Myclass1(id:Int, country:String, points:Int, title:String, variety:String, winery:String)
  case class Myclass2(id:Int,
                      country:String,
                      points:Int,
                      price:Double,
                      title:String,
                      variety:String,
                      winery:String)
  case class Myclass3(id:Int,
                      points:Int,
                      price:Double,
                      title:String,
                      variety:String,
                      winery:String)
  case class Myclass4(id:Int,
                      points:Int,
                      title:String,
                      variety:String,
                      winery:String)
  case class Myclass5(id:Int,
                      country:String,
                      title:String,
                      variety:String,
                      winery:String)
  case class Myclass6(id:Int, country:String)
  case class Myclass7(country:String)
  case class Myclass8(country:String, points:Int, title:String, variety:String)
  case class Myclass9(id:Int,
                      country:String,
                      price:Double,
                      title:String,
                      variety:String,
                      winery:String)
  case class Myclass10(id:Int, country:String, title:String)
  case class Myclass11(id:Int, country:String, winery:String)
  case class Myclass12(id:Int, country:String, price:Double, variety:String, winery:String)
  case class Myclass13(id:Int, country:String, title:String, variety:String)
  case class Myclass14(id:Int, winery:String)
  case class Myclass15(id:Int, country:String, points:Int, price:Double, title:String, winery:String)

  implicit val formats = DefaultFormats
  for (lines <- dataFile) {
    parse(lines) match {
      case JObject(List(
      ("id",_),
      ("country",_),
      ("points",_),
      ("title",_),
      ("variety",_),
      ("winery",_))) => println(parse(lines).extract[Myclass1])
      case JObject(List(
      ("id",_),
      ("country",_),
      ("points",_),
      ("price",_),
      ("title",_),
      ("variety",_),
      ("winery",_))) => println(parse(lines).extract[Myclass2])
      case JObject(List(
      ("id",_),
      ("points",_),
      ("price",_),
      ("title",_),
      ("variety",_),
      ("winery",_))) => println(parse(lines).extract[Myclass3])
      case JObject(List(
      ("id",_),
      ("points",_),
      ("title",_),
      ("variety",_),
      ("winery",_))) => println(parse(lines).extract[Myclass4])
      case JObject(List(
      ("id",_),
      ("country",_),
      ("title",_),
      ("variety",_),
      ("winery",_))) => println(parse(lines).extract[Myclass5])
      case JObject(List(("id",_),("country",_))) => println(parse(lines).extract[Myclass6])
      case JObject(List(("country",_))) => println(parse(lines).extract[Myclass7])
      case JObject(List(("country",_),("points",_),("title",_),("variety",_))) => println(parse(lines).extract[Myclass8])
      case JObject(List(("id",_),("country",_),("price",_),("title",_),("variety",_),("winery",_))) => println(parse(lines).extract[Myclass9])
      case JObject(List(("id",_),("country",_),("title",_))) => println(parse(lines).extract[Myclass10])
      case JObject(List(("id",_),("country",_),("winery",_))) => println(parse(lines).extract[Myclass11])
      case JObject(List(("id",_),("country",_),("price",_),("variety",_),("winery",_))) => println(parse(lines).extract[Myclass12])
      case JObject(List(("id",_),("country",_),("title",_),("variety",_))) => println(parse(lines).extract[Myclass13])
      case JObject(List(("id",_),("winery",_))) => println(parse(lines).extract[Myclass14])
      case JObject(List(("id",_),("country",_),("points",_),("price",_),("title",_),("winery",_))) => println(parse(lines).extract[Myclass15])
    }
  }
}
