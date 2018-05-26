package lastfm

import org.apache.spark.sql.{Dataset, SparkSession}

object PopularSongsSqlJob extends App with SqlJobTemplate[MaxRankArgs, (String, String, Long)] {

  override def jobName: String = "PopularSongsSqlJob"

  override def processTracks(tracks: Dataset[Track], sparkSession: SparkSession, args: MaxRankArgs): Dataset[(String, String, Long)] = {
    import sparkSession.implicits._
    tracks.groupBy('artistName, 'trackName)
      .count()
      .sort('count.desc)
      .limit(args.maxRank)
      .as[(String, String, Long)]
  }

  override def parseArgs(argsArray: Array[String]): MaxRankArgs =
    MaxRankArgs(argsArray)

  run(args)

}