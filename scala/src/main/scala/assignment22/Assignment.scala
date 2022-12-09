package assignment22

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, VectorAssembler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, max, min, transform, udf, when}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import breeze.linalg._
import breeze.plot._
import breeze.numerics

import scala.+:


class Assignment {

  val spark: SparkSession = SparkSession.builder()
    .appName("ex2")
    .config("spark.driver.host", "localhost")
    .master("local")
    .getOrCreate()

  val schema1 = new StructType()
    .add(StructField("a", DoubleType, false))
    .add(StructField("b", DoubleType, false))
    .add(StructField("label",StringType,false))



  val schema2 = new StructType()
    .add(StructField("a", DoubleType, false))
    .add(StructField("b", DoubleType, false))
    .add(StructField("c", DoubleType, false))
    .add(StructField("label", StringType, false))



  // the data frame to be used in tasks 1 and 4


  val dirtyData: DataFrame = spark.read.format("csv").option("header", "true").schema(schema1).load("/home/sayhelloxd/IdeaProjects/casey-haider/scala/data/dataD2_dirty.csv")

  val dataD2: DataFrame = spark.read.format("csv").option("header", "true").schema(schema1).load("/home/sayhelloxd/IdeaProjects/casey-haider/scala/data/dataD2.csv")

  val minmaxAD2 = dataD2.agg(min("a"), max("a")).head()
  val minmaxBD2 = dataD2.agg(min("b"), max("b")).head()

  //dirtyData.filter(col("LABEL").isNotNull && !col("LABEL").contains("null") && !col("LABEL").contains("Unknown") &&col("a").isNotNull&&col("b").isNotNull && col("a")<=1.06882 && col("a") >= -0.98484 && col("b") <= 9.81266 && col("b") >= 0.06052)
  // .filter(!col("LABEL").contains("Unknown"))
  //.filter(dirtyData.col("LABEL").isNull && dirtyData.col("LABEL") == "Unknown" && dirtyData.col("LABEL") == "" && dirtyData.col("LABEL") == "null").
  //filter(dirtyData.col("a").isNull && dirtyData.col("a") != "error" && dirtyData.col("a") == "" && dirtyData.col("a") == "null")
  //.filter(dirtyData.col("b").isNull && dirtyData.col("b") != "error" && dirtyData.col("b") == "" && dirtyData.col("b") == "null")
  //col("a")<1.7 && col("a")>0.9 && col("b")<9.9 && col("b")>0.6052

  // the data frame to be used in task 2
  val dataD3: DataFrame = spark.read.format("csv").option("header", "true").schema(schema2).load("/home/sayhelloxd/IdeaProjects/casey-haider/scala/data/dataD3.csv")

  val minmaxAD3 = dataD3.agg(min("a"), max("a")).head()
  val minmaxBD3 = dataD3.agg(min("b"), max("b")).head()
  val minMaxCD3 = dataD3.agg(min("c"), max("c")).head()

  // the data frame to be used in task 3 (based on dataD2 but containing numeric labels)
  val dataD2WithLabels: DataFrame = dataD2.withColumn("numeric_labels", when(dataD2("LABEL") === "Fatal", 0).otherwise(1))  // REPLACE with actual implementation


  def main(args: Array[String]): Unit = {


  }

  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    //Initializing assembler for data to convert it in vectors format, so that KMeans can work on it
    val assembler = new VectorAssembler().setInputCols(df.columns.filter(_!="label")).setOutputCol("features")

    //Initializing Scalar that will scale features between 0 and 1
    val scalar = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").setMin(0).setMax(1)

    //Initializing KMeans object with seed 1 and k clusters, setting featurescol to scaledFeatures to produced scaled results
    val kMeans = new org.apache.spark.ml.clustering.KMeans()
      .setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
    //Initializing pipeline so it will take assembler, scalar and KMeans object
    val pipeline = new Pipeline().setStages(Array(assembler, scalar, kMeans))
    //fitting the data in pipeline
    val pipelineModel = pipeline.fit(df)
    //Selection last instance which is KMeans in our case to use as an object for KModel
    val KModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    //mapping Array[Array[Double]] to Array[Tuple2[Array]]
    // scaling back a = > a = [0, 1] => [a_min, a_max]
    //new_a = a0* (a_max - a_min) + a_min  for all of features
    var doubleArray: Array[(Double, Double)] = KModel.clusterCenters.map(_.toArray).map { case Array(f1, f2) => ((f1*(minmaxAD2.getDouble(1)-minmaxAD2.getDouble(0))+minmaxAD2.getDouble(0)), (f2 * (minmaxBD2.getDouble(1)-minmaxBD2.getDouble(0)) + minmaxBD2.getDouble(0))) }
    doubleArray
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {

    //Initializing assembler for data to convert it in vectors format, so that KMeans can work on it
    val assembler = new VectorAssembler().setInputCols(df.columns.filter(_ != "label")).setOutputCol("features")

    //Initializing Scalar that will scale features between 0 and 1
    val scalar = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").setMin(0).setMax(1)

    //Initializing KMeans object with seed 1 and k clusters, setting featurescol to scaledFeatures to produced scaled results
    val kMeans = new org.apache.spark.ml.clustering.KMeans()
      .setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
    //Initializing pipeline so it will take assembler, scalar and KMeans object
    val pipeline = new Pipeline().setStages(Array(assembler, scalar, kMeans))
    //fitting the data in pipeline
    val pipelineModel = pipeline.fit(df)
    //Selection last instance which is KMeans in our case to use as an object for KModel
    val KModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    //mapping Array[Array[Double]] to Array[Tuple2[Array]]
    // scaling back a = > a = [0, 1] => [a_min, a_max]
    //new_a = a0* (a_max - a_min) + a_min  for all of features
    var doubleArray: Array[(Double, Double, Double)] = KModel.clusterCenters.map(_.toArray).map { case Array(f1, f2, f3) => ((f1*(minmaxAD3.getDouble(1)-minmaxAD3.getDouble(0))+minmaxAD3.getDouble(0)), (f2 * (minmaxBD3.getDouble(1)-minmaxBD3.getDouble(0)) + minmaxBD3.getDouble(0)) , (f3 * (minMaxCD3.getDouble(1)-minMaxCD3.getDouble(0)) + minMaxCD3.getDouble(0)) ) }
    doubleArray


  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    //Initializing assembler for data to convert it in vectors format, so that KMeans can work on it
    val assembler = new VectorAssembler().setInputCols(df.columns.filter(_ != "label")).setOutputCol("features")

    //Initializing Scalar that will scale features between 0 and 1
    val scalar = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").setMin(0).setMax(1)

    //Initializing KMeans object with seed 1 and k clusters, setting featurescol to scaledFeatures to produced scaled results
    val kMeans = new org.apache.spark.ml.clustering.KMeans()
      .setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
    //Initializing pipeline so it will take assembler, scalar and KMeans object
    val pipeline = new Pipeline().setStages(Array(assembler, scalar, kMeans))
    //fitting the data in pipeline
    val pipelineModel = pipeline.fit(df)
    //Selection last instance which is KMeans in our case to use as an object for KModel
    val KModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    //mapping Array[Array[Double]] to Array[Tuple2[Array]]
    // scaling back a = > a = [0, 1] => [a_min, a_max]
    //new_a = a0* (a_max - a_min) + a_min  for all of features
    var doubleArray: Array[(Double, Double)] = KModel.clusterCenters.map(_.toArray).map { case Array(f1, f2, f3) => (f1, f2, f3) }.filter{case (f1,f2,f3) => f3==0.0}.map{case(f1,f2,f3)=>((f1*(minmaxAD2.getDouble(1)-minmaxAD2.getDouble(0))+minmaxAD2.getDouble(0)),(f2 *(minmaxBD2.getDouble(1)-minmaxBD2.getDouble(0))+minmaxBD2.getDouble(0)))}
    doubleArray.foreach(println)
    doubleArray
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {

    var results : Array[(Int, Double)] = Array[(Int, Double)]()

    def KMeans_(k: Int,  scaledDf: DataFrame): Array[(Int, Double)]= {
      if (k==1) {
        results
      }
      else{
        KMeans_(k-1, scaledDf)
        val kMeans = new org.apache.spark.ml.clustering.KMeans()
          .setK(k).setSeed(1)
        val predictions = kMeans.fit(scaledDf).transform(scaledDf)
        // Evaluate clustering by computing Silhouette score
        val evaluator = new ClusteringEvaluator()
        val silhouetteScore = evaluator.evaluate(predictions)
        results = results :+ (k, silhouetteScore)

        results
      }
    }


    //var results : Array[(Int, Double)] = Array[(Int, Double)]()
    //Transforming DataFrame into Vectors format
    val transformedDf = new VectorAssembler().setInputCols(df.columns.filter(_ != "label")).setOutputCol("features")
      .transform(df)

    //Scaling the features in vectorized DataFrame
    val scalar = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").setMin(0).setMax(1)
    val scalarModel = scalar.fit(transformedDf)
    val scalarTransformed = scalarModel.transform(transformedDf).drop("features").withColumnRenamed("scaledFeatures", "features")
    //Done Through Recursion
    KMeans_(high, scalarTransformed)

    import breeze.linalg._
    import breeze.plot._

    val f = Figure()
    val p = f.subplot(0)
    val x = results.map{case (f1,f2) =>(f1.toDouble)}
    val y = results.map { case (f1, f2) => (f2) }

    p += plot(x,y)
    p.xlabel = "K Values"
    p.ylabel = "Silhoutte Scores"
    Thread.sleep(10000L)
    results
  }


}

/*
  val rows = new VectorAssembler().setInputCols(Array("a", "b")).setOutputCol("features")
    .transform(df)
    .select("features")
    .rdd

  val rdd = rows
    .map(_.getAs[org.apache.spark.ml.linalg.Vector](0))
    .map(org.apache.spark.mllib.linalg.Vectors.fromML)

  val clusters = org.apache.spark.mllib.clustering.KMeans.train(rdd, k, 1)
  //val arr :Array[Double] =clusters.clusterCenters.map(_.toArray).map(_.toArray.map(_.toDouble))
  var doubleArray: Array[(Double,Double)] =clusters.clusterCenters.map(_.toArray).map{case Array(f1,f2) => (f1,f2)}

  doubleArray
 */

/*
    df.printSchema()
    val vectorAssembler: VectorAssembler = new VectorAssembler().setInputCols(Array("a", "b")).setOutputCol("features")
    val assembledDataD2DF: DataFrame = vectorAssembler.transform(df)
    val transformationPipeline = new Pipeline()
      .setStages(Array(vectorAssembler))
    // Fit produces a transformer
    //val pipeLine = transformationPipeline.fit(df)
    //val transformedData = pipeLine.transform(df)
    //transformedData.show()

    val kMeans = new KMeans()
      .setK(k).setSeed(1L)
    val kmModel = kMeans.fit(assembledDataD2DF)
    val predictions = kmModel.transform(assembledDataD2DF)
    predictions.printSchema()
    predictions.show()
    kmModel.clusterCenters.foreach(println)
    kmModel.clusterCenters.map(s=>s.toArray).clone().foreach(println)
    //println(kmModel.clusterCenters.length)
*/

/*
val transformedDf = new VectorAssembler().setInputCols(df.columns.filter(_ != "label")).setOutputCol("features")
  .transform(df)

val scalar = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures").setMin(0).setMax(1)
val scalarModel = scalar.fit(transformedDf)
val scalarTransformed = scalarModel.transform(transformedDf).drop("features").withColumnRenamed("scaledFeatures", "features")

val kMeans = new org.apache.spark.ml.clustering.KMeans()
  .setK(k).setSeed(1)
val kmModel = kMeans.fit(scalarTransformed)
val predictions = kmModel.transform(scalarTransformed)
predictions.show()
kmModel.clusterCenters.foreach(println)

var doubleArray: Array[(Double, Double, Double)] = kmModel.clusterCenters.map(_.toArray).map { case Array(f1, f2, f3) => (f1, f2, f3) }

doubleArray

 */

/*
    for (i <- 2 to 13) {
      val kMeans = new org.apache.spark.ml.clustering.KMeans()
        .setK(i).setSeed(1)
      val predictions = kMeans.fit(scalarTransformed).transform(scalarTransformed)
      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()
      val silhouetteScore = evaluator.evaluate(predictions)
      results = results :+ (i, silhouetteScore)
    }
    results.foreach(println)
*/