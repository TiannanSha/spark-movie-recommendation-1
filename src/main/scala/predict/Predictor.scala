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


  // ****************************
  // ***        Q3.1.4        ***
  // ****************************

  // Note that some intermediate results could have been reused, but for accurately measuring time cost
  // in Q3.1.5, they are not reused
  val yTrue = test.map(r =>  r.rating)
  val rTrue = test.map(r => ((r.user, r.item),r.rating))
  assert(rTrue.count == test.count)

  // both rTrue and rPred are in the form of ((u, i), r), (u,i) is the unique key
  // rTure's r is the actual rating, rPred's r is the predicted rating
  def maeUIR(rTrue:RDD[((Int, Int), Double)], rPred:RDD[((Int, Int), Double)]): Double = {
    assert(rTrue.count == rPred.count)
    val joined = rTrue.join(rPred)  // ((u,i), (r_true, r_pred))
    val maeRdd0 = joined.map{
      case((u,i), (r_true, r_pred))=>scala.math.abs(r_true-r_pred)
    }
    assert(rTrue.count == maeRdd0.count)
    return maeRdd0.sum/rTrue.count.toDouble
  }

  // *** Global Average Method ***
  // use training set's global average to predict each rating in test set
  def calcRPredGlobal(train:RDD[Rating], test:RDD[Rating]):RDD[((Int, Int),Double)] = {
    val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
    test.map(r => ((r.user, r.item), avgGlobal))
  }
  val rPredGlobal = calcRPredGlobal(train, test)
  val maeGlobalMethod = maeUIR(rTrue, rPredGlobal)

  // *** Average Per User Method ***
  // for a test (u,i), if there ratings in training set with same u
  // then use the average of such ratings to predict, otherwise use global average
  def calcRPredPerUserMethod(train:RDD[Rating], test:RDD[Rating]):RDD[((Int, Int),Double)] = {
    val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
    val ru_s = train.groupBy(r => r.user).map{
      case (user, rs) => (user, rs.map(r=>r.rating).sum / rs.size.toDouble)
    }  // (u, ru_)
    val rddPum0 = test.map(r=>(r.user, r.item)) // (u,i)`
    val rddPum1 = rddPum0.leftOuterJoin(ru_s) // (u,(i,Option(ru_)))
    rddPum1.map{
      case(  u, (i, Some(ru))  ) => ((u,i), ru)
      case(  u, (i, None)  ) => ((u,i), avgGlobal)
    } // ((u,i), ru/avgGlobal)
  }
  val rPredPerUserMethod = calcRPredPerUserMethod(train, test)
  val maePerUserMethod = maeUIR(rTrue, rPredPerUserMethod)


  // *** per item method ***
  // for a test (u,i), if there ratings in training set with same i
  // then use the average of such ratings to predict, otherwise use global average
  def calcRPredPerItemMethod(train:RDD[Rating], test:RDD[Rating]):RDD[((Int, Int),Double)] = {
    // (i, [Rating]) ->  (i, [r]) -> (i, [r].sum/r.size)
    val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
    val r_is = train.groupBy(r => r.item).map{
      case (item, rs) => (item, rs.map(r=>r.rating).sum / rs.size.toDouble)
    } // (i, r_i)
    val rddPim0 = test.map(r=>(r.item, r.user))  // (i, u)
    val rddPim1 = rddPim0.leftOuterJoin(r_is)  // (i, (u, Option(r_i)))
     rddPim1.map{
      case(i, (u, Some(r_i))) => ((u,i), r_i)
      case(i, (u, None)) => ((u,i), avgGlobal)
    } // ((u,i), r_i/avgGlobal)
  }
  val rPredPerItemMethod = calcRPredPerItemMethod(train, test)
  val maePerItemMethod = maeUIR(rTrue, rPredPerItemMethod)

  //TODO: delete redundant rdd12345 variables change it to new line .func()
  // *** the baseline method ***
  // as explained in the project specification
//  def calcRPredBaselineMethod(train:RDD[Rating], test:RDD[Rating]):RDD[((Int, Int),Double)] = {
//
//    // find avgGlobal and ru_s
//    val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
//    val ru_s = train.groupBy(r => r.user).map{
//      case (user, rs) => (user, rs.map(r=>r.rating).sum / rs.size.toDouble)
//    }  // (u, ru_)
//
//    // find rHatBar_i for all is
//    val rdd1 = train.map(r=>(r.user, (r.item, r.rating)))   // entry: (u, (i, rui))
//    val rdd2 = rdd1.join(ru_s)  // entry (u, ((i, rui), ru_))
//    val rdd3 = rdd2.map{case(  u, ((i, rui),ru_)  ) => ( i, (normalDevi(rui,ru_),1) )} // (i, (rhat_ui, 1))
//    // after groupby it's (i, [(rhat_u1_i,1), (rhat_u2_i,1), ...])
//    // for better performance we reduce directly
//    // after reduce: (i, (rhat_ui+rhat_u2i2+..., 1+1+...))
//    val rHatBar_i = rdd3.reduceByKey((t1,t2)=>(t1._1+t2._1, t1._2+t2._2)).mapValues{
//      case(sum, count) => sum/count.toDouble
//    }  // (i, rhatbar_i)
//
//    // now combine rHatBar_i and ru_ for each entry in the testset
//    val rdd4 = test.map{r=>(r.item, r.user)}.leftOuterJoin(rHatBar_i) // (i, (u1, Option(rhatbar_i)))
//    val rdd5 = rdd4.map{
//      case(i, (u, rbarhat_i)) => (u, (i, rbarhat_i))
//    } // (u, (i, Option(rbarhat_i))
//    val rdd6 = rdd5.leftOuterJoin(ru_s) // (   u, ( (i, Option(rbarhat_i)), option(ru_) )   )
//    rdd6.map{
//      case( u, (  (i,rbarhat_i), ru  ) ) => ( (u,i), optionalPui(ru, rbarhat_i, avgGlobal) )
//    }  // ((u,i), pui)
//  }
  // *** the baseline method ***
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
  val rPredBaselineMethod = calcRPredBaselineMethod(train, test)
  val maeBaselineMethod = maeUIR(rTrue, rPredBaselineMethod)

  // Some helper functions for the baseline method
  // generate a prediction for (u,i) using ru_ and rbarhat_i while any of
  // these two inputs might be None, in which case the prediction will be the global average
  def optionalPui(ru:Option[Double], rbarhat_i:Option[Double], avgGlobal:Double):Double = {
//    var ruGet = avgGlobal
//    var rbarhat_iGet = avgGlobal
    if (ru.isEmpty && rbarhat_i.isEmpty) {
      return pui(avgGlobal, avgGlobal)
    } else if (ru.isDefined && rbarhat_i.isEmpty) {
      return pui(ru.get, avgGlobal)
    } else if (ru.isEmpty && rbarhat_i.isDefined) {
      return pui(avgGlobal, rbarhat_i.get)
    } else {
      return pui(ru.get, rbarhat_i.get)
    }
  }

  // generate a prediction for (u,i) using ru_ and rbarhat_i
  def pui(ru:Double, rbarhat_i:Double):Double = {
    ru + rbarhat_i * scale((ru+rbarhat_i), ru)
  }

  // normalizedDeviation
  def normalDevi(rui:Double, ru:Double): Double = {
    (rui - ru)/scale(rui, ru)
  }

  def scale(x:Double, ru:Double): Double = {
    if (x>ru) {
      5-ru
    } else if (x<ru) {
      ru-1
    }else {
      1
    }
  }

  // ****************************
  // ***        Q3.1.5        ***
  // ****************************

  // for a given predictive method, run it for turn times, time each run and
  // return ten durations in a RDD[Long] in microseconds
  def timeMethod(methodFunc:(RDD[Rating], RDD[Rating])=>RDD[((Int, Int), Double)],
                train:RDD[Rating], test:RDD[Rating]) : RDD[Double] = {
    var timeListGlobalMethod = List[Double]()
    for (i <- 1 to 10) {
      val start = System.nanoTime()
      methodFunc(train, test)
      val end = System.nanoTime()
      // divide by 1000 to turn nano seconds to microseconds
      timeListGlobalMethod = timeListGlobalMethod :+ (end-start)/1000.0
    }
    spark.sparkContext.parallelize(timeListGlobalMethod)
  }

  // *** measure global average method's duration ***
  val timeRddGlobalMethod = timeMethod(calcRPredGlobal, train, test)

  // *** measure per user method's duration ***
  val timeRddPerUserMethod = timeMethod(calcRPredPerUserMethod, train, test)

  // *** measure per item method's duration ***
  val timeRddPerItemMethod = timeMethod(calcRPredPerItemMethod, train, test)

  // *** measure baseline method's duration ***
  val timeRddBaselineMethod = timeMethod(calcRPredBaselineMethod, train, test)


// TODO: clean this up
//  var timeListGlobalMethod = List[Long]()
//  for (i <- 1 to 10) {
//    val start = System.nanoTime()
//    calcRPredGlobal(train, test)
//    val end = System.nanoTime()
//    timeListGlobalMethod = timeListGlobalMethod :+ (end-start)
//  }
//  val timeRddGlobalMethod = spark.sparkContext.parallelize(timeListGlobalMethod)

  // *** measure per user method's duration ***
//  var timeListPerUserMethod = List[Long]()
//  for (i <- 1 to 10) {
//    val start = System.nanoTime()
//    calcRPredGlobal(train, test)
//    val end = System.nanoTime()
//    timeListPerUserMethod = timeListPerUserMethod :+ (end-start)
//  }
//  val timeRddPerUserMethod = spark.sparkContext.parallelize(timeListPerUserMethod)

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
              "MaeGlobalMethod" -> maeGlobalMethod, // Datatype of answer: Double
              "MaePerUserMethod" -> maePerUserMethod, // Datatype of answer: Double
              "MaePerItemMethod" -> maePerItemMethod, // Datatype of answer: Double
              "MaeBaselineMethod" -> maeBaselineMethod // Datatype of answer: Double
            ),

            "Q3.1.5" -> Map(
              "DurationInMicrosecForGlobalMethod" -> Map(
                "min" -> timeRddGlobalMethod.min,  // Datatype of answer: Double
                "max" -> timeRddGlobalMethod.max,  // Datatype of answer: Double
                "average" -> timeRddGlobalMethod.mean, // Datatype of answer: Double
                "stddev" -> timeRddGlobalMethod.stdev// Datatype of answer: Double
              ),
              "DurationInMicrosecForPerUserMethod" -> Map(
                "min" -> timeRddPerUserMethod.min,  // Datatype of answer: Double
                "max" -> timeRddPerUserMethod.max,  // Datatype of answer: Double
                "average" -> timeRddPerUserMethod.mean, // Datatype of answer: Double
                "stddev" -> timeRddPerUserMethod.stdev // Datatype of answer: Double
              ),
              "DurationInMicrosecForPerItemMethod" -> Map(
                "min" -> timeRddPerItemMethod.min,  // Datatype of answer: Double
                "max" -> timeRddPerItemMethod.max,  // Datatype of answer: Double
                "average" -> timeRddPerItemMethod.mean, // Datatype of answer: Double
                "stddev" -> timeRddPerItemMethod.stdev // Datatype of answer: Double
              ),
              "DurationInMicrosecForBaselineMethod" -> Map(
                "min" -> timeRddBaselineMethod.min,  // Datatype of answer: Double
                "max" -> timeRddBaselineMethod.max,  // Datatype of answer: Double
                "average" -> timeRddBaselineMethod.mean, // Datatype of answer: Double
                "stddev" -> timeRddBaselineMethod.stdev // Datatype of answer: Double
              ),
              "RatioBetweenBaselineMethodAndGlobalMethod" -> (
                timeRddBaselineMethod.mean/timeRddGlobalMethod.mean) // Datatype of answer: Double
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
