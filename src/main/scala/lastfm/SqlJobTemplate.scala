package lastfm

import lastfm.Timer.timed
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

trait SqlJobTemplate[Arg <: Args, Out] extends Logging {

  def jobName: String

  def processTracks(tracks: Dataset[Track], sparkSession: SparkSession, args: Arg): Dataset[Out]

  def parseArgs(argsArray: Array[String]): Arg

  def run(argsArray: Array[String]): Unit = {
    val args = parseArgs(argsArray)
    val sparkSession = SparkSession.builder().appName(jobName).getOrCreate()
    logger.info(s"loading tracks from ${args.inputPath} and writing out to ${args.outputPath}")
    timed {
      val tracks = RecentTracksReader.recentTracksDataset(sparkSession, args.inputPath)
      val results = processTracks(tracks, sparkSession, args)
      results.write.csv(args.outputPath)
    }
  }

}