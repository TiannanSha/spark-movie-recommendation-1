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

  // *** Q3.1.1 ***
  val gloAvgRating = data.map(r=>r.rating).mean

  // *** Q3.1.2 ***
  // as per milestone spec, ru_ denotes average rating of user u
  val ru_s = data.map(  r => (r.user, (r.rating,1))  ) // (u,r) -> (u,[(r1,1), (r2,1)])
    .reduceByKey( (t1,t2) => (t1._1+t2._1, t1._2+t2._2)) // (u, (r+r+r..., 1+1+1)) = (u, sum/count)
    .mapValues{ case(sum, count) => sum/count}
  // final answers
  val ru_sMin = ru_s.values.min
  val ru_sMax = ru_s.values.max
  val ru_sMean = ru_s.values.mean
  val ratioCloseUser = ru_s.values.filter(
      r => (r - gloAvgRating).abs < 0.5
    ).count / ru_s.count.toDouble
  // if all users are close to the global average, the ratio should be 1.
  val allCloseUser = (ratioCloseUser==1)


  // *** Q3.1.3 ***
  // as per milestone spec, r_i denotes average rating of item i
  // r_is is the pural form of r_i...
  val r_is = data.map(  r => (r.item, (r.rating,1))  ) // (i,r) -> (i,[(r1,1), (r2,1)])
    .reduceByKey( (t1,t2) => (t1._1+t2._1, t1._2+t2._2)) // (i, (r+r+r..., 1+1+1)) = (i, sum/count)
    .mapValues{ case(sum, count) => sum/count}
  // final answers
  val r_isMin = r_is.values.min
  val r_isMax = r_is.values.max
  val r_isMean = r_is.values.mean
  val ratioCloseItem = r_is.values.filter(
    r => (r - gloAvgRating).abs < 0.5
  ).count / r_is.count.toDouble
  // if all items are close to the global average, the ratio should be 1.
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
                "min" -> ru_sMin,  // Datatype of answer: Double
                "max" -> ru_sMax, // Datatype of answer: Double
                "average" -> ru_sMean // Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> allCloseUser, // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> ratioCloseUser // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> r_isMin,  // Datatype of answer: Double
                "max" -> r_isMax, // Datatype of answer: Double
                "average" -> r_isMean // Datatype of answer: Double
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
