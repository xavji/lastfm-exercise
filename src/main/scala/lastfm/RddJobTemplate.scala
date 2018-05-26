package lastfm

import lastfm.Timer.timed
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

trait RddJobTemplate[Arg <: Args, Out] extends Logging {

  def jobName: String

  def processTracks(tracks: RDD[Track], sparkContext: SparkContext, args: Arg): RDD[Out]

  def parseArgs(argsArray: Array[String]): Arg

  def run(argsArray: Array[String], rowToCsvLine: Out => String): Unit = {
    val args = parseArgs(argsArray)
    val sparkContext = SparkContext.getOrCreate(new SparkConf().setAppName(jobName))
    logger.info(s"loading tracks from ${args.inputPath} and writing out to ${args.outputPath}")
    timed {
      val tracks = RecentTracksReader.recentTracksRDD(sparkContext, args.inputPath)
      val results = processTracks(tracks, sparkContext, args)
      results.map(rowToCsvLine).saveAsTextFile(args.outputPath)
    }
  }

}