package stats

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  // use r to denote Rating class and rating to denote actual numerical rating

  // Q3.1.1
  val numRating = data.count
  val gloAvgRating = data.map(r=>r.rating).sum/numRating

  // Q3.1.2
  //val avgRatingPerUser = data.groupBy(r=>r.user).map(group => group.map(r=>r.ratings)/group.count )
  //var avgRatingPerUser = data.groupBy(r => r.user).map{case (user, rs) => (rs.sum/rs.count)}
  val userRatingsMap = data.groupBy(r => r.user).map{case (user, rs) => (user, rs.map(r=>r.rating))}
  //userRatingsMap.take(10).foreach(println)
  val userAvgRatings= userRatingsMap.map{case (user, ratings) => (ratings.sum/ratings.size)}
  //userAvgRatings.collect.foreach(println)
  val userAvgRatingsMax = userAvgRatings.max
  val userAvgRatingsMin = userAvgRatings.min
  val userAvgRatingsAvg = userAvgRatings.sum/userAvgRatings.count
  val ratioCloseUser = (userAvgRatings.filter(rating => (rating - userAvgRatingsAvg).abs < 0.5).count.toDouble
    /userAvgRatings.count)  // *100?
  val allCloseUser = (ratioCloseUser==1)

  // Q3.1.3
  val itemRatingsMap = data.groupBy(r => r.item).map{case (item, rs) => (item, rs.map(r=>r.rating))}
  //userRatingsMap.take(10).foreach(println)
  // get a sequence of average ratings for each item
  val itemAvgRatings= itemRatingsMap.map{case (item, ratings) => (ratings.sum/ratings.size)}
  println("*****itemAvgRatings*****")
  itemAvgRatings.collect.foreach(println)
  val itemAvgRatingsMax = itemAvgRatings.max
  val itemAvgRatingsMin = itemAvgRatings.min
  val itemAvgRatingsAvg = itemAvgRatings.sum/itemAvgRatings.count
  val ratioCloseItem = (itemAvgRatings.filter(rating => (rating - itemAvgRatingsAvg).abs < 0.5).count.toDouble
    /userAvgRatings.count)  // *100?
  val allCloseItem = (ratioCloseItem==1)

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> gloAvgRating // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> userAvgRatingsMin,  // Datatype of answer: Double
                "max" -> userAvgRatingsMax, // Datatype of answer: Double
                "average" -> userAvgRatingsAvg // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> allCloseUser, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratioCloseUser // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> itemAvgRatingsMin,  // Datatype of answer: Double
                "max" -> itemAvgRatingsMax, // Datatype of answer: Double
                "average" -> itemAvgRatingsAvg // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> allCloseItem, // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> ratioCloseItem // Datatype of answer: Double
          ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
