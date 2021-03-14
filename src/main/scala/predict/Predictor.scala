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
  val rTrue = test.map(r => ((r.user, r.item),r.rating))
  assert(rTrue.count == test.count)

  def MAE(yTrue:RDD[Double], yPred:RDD[Double]): Double ={
    assert(yTrue.count == yPred.count)
    val zipped = yTrue.zip(yPred)
    //joined.take(8).foreach(print)
    zipped.map(zipped => scala.math.abs(zipped._1 - zipped._2)).sum() / yTrue.count.toDouble
//    joined.map{
//      case (_, (rTrue, rPred)) => scala.math.abs(rTrue - rPred)
//    }.sum() / u_yTrue.count.toDouble
  }

  // both rTrue and rPred are in the form of ((u, i), rating)
  def mae(rTrue:RDD[((Int, Int), Double)], rPred:RDD[((Int, Int), Double)]): Double = {
    assert(rTrue.count == rPred.count)
    val joined = rTrue.join(rPred)  // ((u,i), (r_true, r_pred))
    val maeRdd0 = joined.map{
      case((u,i), (r_true, r_pred))=>scala.math.abs(r_true-r_pred)
    }
    assert(rTrue.count == maeRdd0.count)
    return maeRdd0.sum/rTrue.count.toDouble
  }

  // MAE for global method, use training set's global average to predict each rating in test set
  val avgGlobal = train.map(r => r.rating).sum()/train.count.toDouble
  val yPredGlobal = test.map(r => avgGlobal)
  val MaeGlobalMethod = MAE(yTrue, yPredGlobal)
  println(s"MAE(yTrue, yTrue) = ${MAE(yTrue, yTrue)}")

  // MAE for per user method, for a test (u,i), if there ratings in training set with same u
  // then use the average of such rating to predict, otherwise use global average
  val ru_s = train.groupBy(r => r.user).map{
    case (user, rs) => (user, rs.map(r=>r.rating).sum / rs.size.toDouble)
  }  // (u, ru_)

  val rddPum0 = test.map(r=>(r.user, r.item)) // (u,i)
  val rddPum1 = rddPum0.leftOuterJoin(ru_s) // (u,(i,Option(ru_)))
  val rPredPerUserMethod = rddPum1.map{
    case(  u, (i, Some(ru))  ) => ((u,i), ru)
    case(  u, (i, None)  ) => ((u,i), avgGlobal)
  } // ((u,i), ru/avgGlobal)
  val maePerUserMethod = mae(rTrue, rPredPerUserMethod)


  // MAE for per item method
  // (i, [Rating]) ->  (i, [r]) -> (i, [r].sum/r.size)
  val r_is = train.groupBy(r => r.item).map{
    case (item, rs) => (item, rs.map(r=>r.rating).sum / rs.size.toDouble)
  } // (i, r_i)
  val rddPim0 = test.map(r=>(r.item, r.user))  // (i, u)
  val rddPim1 = rddPim0.leftOuterJoin(r_is)  // (i, (u, Option(r_i)))
  val rPredPerItemMethod = rddPim1.map{
    case(i, (u, Some(r_i))) => ((u,i), r_i)
    case(i, (u, None)) => ((u,i), avgGlobal)
  } // ((u,i), r_i/avgGlobal)
  val maePerItemMethod = mae(rTrue, rPredPerItemMethod)


  // baseline method
  // attempt to use
//  val rdd1 = train.map(r=>(r.item, r)) // (item2, r)
//  val rdd2 = rdd1.groupByKey // (item2, [r1,r2,...])
//  val rdd3 = rdd2.mapValues(rs=>rs.map(r=>(r.user, r.rating))) // (item2, [(u1, rating1), (u2,rating2), ...])
//  val rdd4 = rdd3.mapValues(u_r_s => spark.sparkContext.parallelize(u_r_s.toSeq))
//  // rdd4 entry: (item2, RDD((u1, r12),(u2,r22),...)])
//  val rdd5 = rdd4.mapValues(u_r_s=>u_r_s.join(avgRPerUser))
//  // rdd5 entry: (  item2, RDD( (u1,(r12, avgR_u1)), (u2,(r22, avgR_u2)),...)   )
//  val rdd6 = rdd5.mapValues{
//    rdd => rdd.mapValues{case(rui, ru)=>normalDevi(rui, ru)}
//  } // rdd6 entry (   item2, RDD( (u1,rhat_u1_i2 ), (u2, rhat_u2_i2) )       )
//  val rdd7 = rdd6.mapValues{rdd=>rdd.values.sum}
//  //rdd7 entry ( item2, rhatbar_i2 )
//  println("befroe print rdd7...")
//  rdd7.take(10).foreach(println)

  val rdd1 = train.map(r=>(r.user, (r.item, r.rating)))   // entry: (u, (i, rui))
  val rdd2 = rdd1.join(ru_s)  // entry (u, ((i, rui), ru_))
  val rdd3 = rdd2.map{case(  u, ((i, rui),ru_)  ) => ( i, (normalDevi(rui,ru_),1) )} // (i, (rhat_ui, 1))
  // after groupby it's (i, [(rhat_u1_i,1), (rhat_u2_i,1), ...])
  // for better performance we reduce directly
  // after reduce: (i, (rhat_ui+rhat_u2i2+..., 1+1+...))
  val rHatBar_i = rdd3.reduceByKey((t1,t2)=>(t1._1+t2._1, t1._2+t2._2)).mapValues{
    case(sum, count) => sum/count.toDouble
  }
//  println()
//  rHatBar_i.take(10).foreach(println)
//  val debugRHatBar_i = rHatBar_i.map(t=>(t._2, t._1))
//  println(s"debugRHatBar_i.max=${debugRHatBar_i.max}")
//  println(s"debugRHatBar_i.min=${debugRHatBar_i.min}")
//  println()
  // now combine rHatBar_i and ru_ for each entry in the testset
  val rdd4 = test.map{r=>(r.item, r.user)}.leftOuterJoin(rHatBar_i) // (i, (u1, Option(rhatbar_i)))
  println("rdd4: (i, (u, Option(rhatbar_i)))")
  rdd4.take(10).foreach(println)
  println()
  val rdd5 = rdd4.map{
    case(i, (u, rbarhat_i)) => (u, (i, rbarhat_i))
  } // (u, (i, Option(rbarhat_i))
  // debug
//  val rddDebug0 = rdd5.map{case(u, (i, riOrGA))=>riOrGA}
//  println("rbarhatiOrGA")
//  rddDebug0.take(10).foreach(println)
//  println(s"rbarhatiOrGA max = ${rddDebug0.max}")
//  println(s"rbarhatiOrGA min = ${rddDebug0.min}")
//  println()
  // debug
  val rdd6 = rdd5.leftOuterJoin(ru_s) // (   u, ( (i, rbarhat_i/GA), option(ru_) )   )
  val rPredBaselineMethod = rdd6.map{
    case( u, (  (i,rbarhat_i), ru  ) ) => ( (u,i), optionalPui(ru, rbarhat_i) )
  }  // ((u,i), pui)
//  println("ru_s: (u, ru_)")
//  ru_s.take(10).foreach(println)
//  val debugRu_s = ru_s.map{case(u, ru)=>ru}
//  println(s"ru_s.max=${debugRu_s.max}")
//  println(s"ru_s.min=${debugRu_s.min}")
//  println()
//  println("rdd7: ((u,i),pui)")
//  rdd7.take(10).foreach(println)
//  val rddDebugPui = rdd7.map{case((u,i),pui)=>pui}
//  println(s"pui max=${rddDebugPui.max}")
//  println(s"pui min=${rddDebugPui.min}")
//  println()
//  val rdd8 = test.map(r=>((r.user, r.item),1))  // ((u,i),1)
//  assert(rdd8.count == rdd7.count)
//  val rdd9 = rdd8.join(rdd7)   // ((u,i), (1,pui))
//  val rPredBaseline = rdd9.map{
//    case((u,i),(one, pui)) => (())
//  }
  val maeBaselineMethod = mae(rTrue, rPredBaselineMethod)
  //println(s"MaeBaseline = $maeBaseline")

  // generate a prediction for (u,i) using ru_ and rbarhat_i while any of
  // these two inputs might be None, in which case the prediction will be the global average
  def optionalPui(ru:Option[Double], rbarhat_i:Option[Double]):Double = {
    if (ru.isEmpty || rbarhat_i.isEmpty) {
      return avgGlobal
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
              "MaePerUserMethod" -> maePerUserMethod, // Datatype of answer: Double
              "MaePerItemMethod" -> maePerItemMethod, // Datatype of answer: Double
              "MaeBaselineMethod" -> maeBaselineMethod // Datatype of answer: Double
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
