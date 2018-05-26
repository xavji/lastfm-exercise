package lastfm

import org.apache.spark.sql.{Dataset, SparkSession}

object DistinctSongsByUserSqlJob extends App with SqlJobTemplate[DistinctSongsArgs, (String, Long)] {

  override def jobName: String = "DistinctSongsByUserSqlJob"

  override def parseArgs(argsArray: Array[String]): DistinctSongsArgs =
    DistinctSongsArgs(argsArray)

  override def processTracks(tracks: Dataset[Track], sparkSession: SparkSession, args: DistinctSongsArgs): Dataset[(String, Long)] = {
    import sparkSession.implicits._
    tracks.select('userId, 'artistName, 'trackName)
      .distinct()
      .groupBy('userId)
      .count()
      .as[(String, Long)]
  }

  run(args)

}