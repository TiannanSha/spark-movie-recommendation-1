package predict

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Predictor extends App {
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
  println("Loading training data from: " + conf.train()) 
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(train.count == 80000, "Invalid training data")

  println("Loading test data from: " + conf.test()) 
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(test.count == 20000, "Invalid test data")

  val globalPred = 3.0 // What is this??
  val globalMae = test.map(r => scala.math.abs(r.rating - globalPred)).reduce(_+_) / test.count.toDouble

  // my code starts here
  println("test take 8")
  test.take(8).foreach(print)

  // *** Q3.1.4 ***
  val yTrue = test.map(r =>  r.rating)
  def MAE(yTrue:RDD[Double], yPred:RDD[Double]): Double ={
    assert(yTrue.count == yPred.count)
    val zipped = yTrue.zip(yPred)
    //joined.take(8).foreach(print)
    zipped.map(zipped => scala.math.abs(zipped._1 - zipped._2)).sum() / yTrue.count.toDouble
//    joined.map{
//      case (_, (rTrue, rPred)) => scala.math.abs(rTrue - rPred)
//    }.sum() / u_yTrue.count.toDouble
  }

  // MAE global method, use training set's global average to predict each rating in test set
  val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
  val yPredGlobal = test.map(r => avgGlobal)
  val MaeGlobalMethod = MAE(yTrue, yPredGlobal)
  println(s"MAE(yTrue, yTrue) = ${MAE(yTrue, yTrue)}")

  // MAE per user method, for a test (u,i), if there ratings in training set with same u
  // then use the average of such rating to predict, otherwise use global average
  val avgRByU = train.groupBy(r => r.user).map{
    case (user, rs) => (user, rs.map(r=>r.rating).sum / rs.size.toDouble)
  }

//  def predict(r:Rating): Double = {
//    val pred = avgRByU.lookup(r.user)
//    if (pred.size > 0) return pred(0) else avgGlobal
//  }
//  println("before lookup")
  //val yPredPerUser = test.map(_=>0.0)
  //val MaePerUserMethod = MAE(yTrue, yPredPerUser)

  // test = [(1, mv1), (1, mv2), (2, mv3), (4,mv5)]
  // e.g. testMapped = [(1, GloAvg), (1, GA), (2, GA), (4,GA)]
  //    avgRbyU = [(1, 3.3), (2, 3.9), (3, 4.0)]
  //  joined = [(1, (GA, 3.3)), (1, (GA, 3.3)), (2, (GA, 3.9)), (4, (GA, None))]
  val testMappedUser = test.map(r=>(r.user, avgGlobal))
  val joinedUser = testMappedUser.leftOuterJoin(avgRByU)
  val yPredPerUser = joinedUser.map{
    case(_, (_, Some(rating))) => rating
    case(_, (gloAvg, None)) => gloAvg
  }
  val MaePerUserMethod = MAE(yTrue, yPredPerUser)
  println(s"MaePerUserMethod = $MaePerUserMethod")



  //val yPredPerUser = test.map(_ => stats.Analyzer.userAvgRatingsAvg)
//  val yPredPerUser = stats.Analyzer.userAvgRatings
//  val MaePerUserMethod = MAE(yTrue, yPredPerUser)
//  // MAE per item method
//  val yPredPerItem = stats.Analyzer.itemAvgRatings
//  val MaePerItemMethod = MAE(yTrue, yPredPerItem)
  val avgRByI = train.groupBy(r => r.item).map{
    case (item, rs) => (item, rs.map(r=>r.rating).sum / rs.size.toDouble)
  }
  val testMappedItem = test.map(r=>(r.item, avgGlobal))
  val joinedItem = testMappedItem.leftOuterJoin(avgRByI)
  val yPredPerItem = joinedItem.map{
    case(_, (_, Some(rating))) => rating
    case(_, (gloAvg, None)) => gloAvg
  }
  val MaePerItemMethod = MAE(yTrue, yPredPerItem)
  println(s"MaePerItemMethod = $MaePerItemMethod")



  // **** Q3.1.5 ****

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
            "Q3.1.4" -> Map(
              "MaeGlobalMethod" -> MaeGlobalMethod, // Datatype of answer: Double
              "MaePerUserMethod" -> MaePerUserMethod, // Datatype of answer: Double
              "MaePerItemMethod" -> MaePerItemMethod, // Datatype of answer: Double
              "MaeBaselineMethod" -> 0.0 // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0, // Datatype of answer: Double
                "stddev" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0, // Datatype of answer: Double
                "stddev" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0,  // Datatype of answer: Double
                "average" -> 0.0, // Datatype of answer: Double
                "stddev" -> 0.0 // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> 0.0,  // Datatype of answer: Double
                "max" -> 0.0, // Datatype of answer: Double
                "average" -> 0.0, // Datatype of answer: Double
                "stddev" -> 0.0 // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> 0.0 // Datatype of answer: Double
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
