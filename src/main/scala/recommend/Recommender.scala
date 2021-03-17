package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import predict.Predictor.{normalDevi, optionalPui}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

object Recommender extends App {
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

  println("Loading personal data from: " + conf.personal()) 
  val personalFile = spark.sparkContext.textFile(conf.personal())
  // TODO: Extract ratings and movie titles
  assert(personalFile.count == 1682, "Invalid personal data")

  // ***************************
  // ***        Q4.1.1       ***
  // ***************************
  val myUserId = 944
  val myRatings = personalFile.map(l => l.split(",").map(_.trim))
    .filter(cols=>cols.length==3)
    .map(cols => Rating(myUserId, cols(0).toInt, cols(2).toDouble))  // col(0) is item id

  // make the training set to be all the (u,i,r) available to get best performance, i.e.
  // all other users' ratings plus my ratings
  // the test set we need to predict is me and all my unrated items
  val train = (data ++ myRatings)
  val toPredict = personalFile.map(l => l.split(",").map(_.trim))
    .filter(cols=>cols.length!=3)
    .map(cols => Rating(myUserId, cols(0).toInt, -1.0))
  val itemNameMap = personalFile.map(l => l.split(",").map(_.trim))
    .filter(cols=>cols.length!=3)
    .map(cols => (cols(0).toInt, cols(1)))  // (i, itemName)

  // get top 5 rated items, break tie by picking items with smaller ids
  val top5 = calcRPredBaselineMethod(train, toPredict) // ((u,i), rating)
    .map{case((u,i),rating) => (rating, -i)} // prefer higher rating and smaller itemId
    .top(5)
    .map{case(rating, negI)=>(rating, -negI)}
  val finalListQ411 = top5ToFinalList(top5)

  // turn the rdd stores top 5 recommended items to the form of the final output
  def top5ToFinalList(top: Array[(Double, Int)]):List[Any] = {
    var finalList = List[Any]()
    for (i <- 0 until top.length) {
      var entryList = List[Any]()
      entryList = entryList :+ top(i)._2 :+ itemNameMap.lookup(top(i)._2)(0):+ top(i)._1
      finalList = finalList :+ entryList
    }
    finalList
  }

  // the baseline method
  // as explained in the project specification
  def calcRPredBaselineMethod(train:RDD[Rating], test:RDD[Rating]):RDD[((Int, Int),Double)] = {

    // find avgGlobal and ru_s
    val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
    val ru_s = train.groupBy(r => r.user).map{
      case (user, rs) => (user, rs.map(r=>r.rating).sum / rs.size.toDouble)
    }  // (u, ru_)

    // find rHatBar_i for all is
    val rdd1 = train.map(r=>(r.user, (r.item, r.rating)))   // entry: (u, (i, rui))
      .join(ru_s)  // entry (u, ((i, rui), ru_))
      .map{case(  u, ((i, rui),ru_)  ) => ( i, (normalDevi(rui,ru_),1) )} // (i, (rhat_ui, 1))
    // after groupby it's (i, [(rhat_u1_i,1), (rhat_u2_i,1), ...])
    // after reduce: (i, (rhat_ui+rhat_u2i2+..., 1+1+...))
    val rHatBar_i = rdd1.reduceByKey((t1,t2)=>(t1._1+t2._1, t1._2+t2._2)).mapValues{
      case(sum, count) => sum/count.toDouble
    }  // (i, rhatbar_i)

    // now combine rHatBar_i and ru_ for each entry in the testset
    test.map{r=>(r.item, r.user)}.leftOuterJoin(rHatBar_i) // (i, (u1, Option(rhatbar_i)))
      .map{
        case(i, (u, rbarhat_i)) => (u, (i, rbarhat_i))
      } // (u, (i, Option(rbarhat_i))
      .leftOuterJoin(ru_s) // (   u, ( (i, Option(rbarhat_i)), option(ru_) )   )
      .map{
        case( u, (  (i,rbarhat_i), ru  ) ) => ( (u,i), optionalPui(ru, rbarhat_i, avgGlobal) )
      }  // ((u,i), pui)
  }

  // ***********************
  // ***     Q4.1.2      ***
  // ***********************
  val countByI = train.map(r=>(r.item, 1)).reduceByKey((a,b)=>a+b)
  val countByIStats = countByI.map{case(i, count) => count}.stats()
  val adjustedTop5 = calcRPredBaselineMethod(train, toPredict)  // ((u,i), pui)
    .map{ case((u,i),r) => (i,r) } // (i, pui)
    .join(countByI) //(i, (pui, count_i))
    .mapValues{case(pui, count_i) => adjustedPui(pui, count_i)}  // (i, adjustedPui)
    .join(itemNameMap) // (i, (adjustedPui, itemName))
    .map{case(i, (adjustedPui, itemName)) => (adjustedPui, -i)} // (adjustedPui, -i)
    .top(5) // -i is used to break tie so that smaller item ids are preferred
    .map{case(adjustedPui, negI) => (adjustedPui, -negI)} // (adjustedPui, i)
  val finalListQ412 = top5ToFinalList(adjustedTop5)

  // based on popularity relative to the most rated item, adjust the pui
  def adjustedPui(pui:Double, count_i:Int):Double = {
    // first normalize count to between (0,1], then smooth it by raising it to the power of 1/16
    val popularityFactor = scala.math.pow(count_i.toDouble/countByIStats.max, 0.0625)
    val adjusted = popularityFactor * pui
    if (adjusted < 1.0) 1.0
    else adjusted
  }

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

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.

            "Q4.1.1" -> finalListQ411,
            "Q4.1.2" -> finalListQ412
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
