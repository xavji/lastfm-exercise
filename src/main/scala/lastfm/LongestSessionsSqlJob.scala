package lastfm

import java.sql.Timestamp

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.unsafe.types.CalendarInterval

object LongestSessionsSqlJob extends App with SqlJobTemplate[MaxRankArgs, (String, Timestamp, Timestamp, String)] {

  override def jobName: String = "LongestSessionsSqlJob"

  override def processTracks(tracks: Dataset[Track], sparkSession: SparkSession, args: MaxRankArgs): Dataset[(String, Timestamp, Timestamp, String)] = {
    import sparkSession.implicits._
    val upperBound = lit(CalendarInterval.fromString("interval 20 minutes"))
    val rankingWindow = Window.partitionBy('userId).orderBy('playedOn).rangeBetween(currentRow(), upperBound)
    val endColumn = max('playedOn).over(rankingWindow)

    val tracksWithNext = tracks.withColumn("end", endColumn)

    val sessions = new SessionFromTracksUdaf
    val sessionLength = udf[Long, Row] { row: Row =>
      row.getTimestamp(SessionFields.SessionEndIndex).getTime - row.getTimestamp(SessionFields.SessionStartIndex).getTime
    }

    val sessionsWithLength = tracksWithNext.groupBy('userId)
      .agg(sessions('userId, 'playedOn, 'end, 'artistName, 'trackName).as("sessions"))
      .withColumn("session", explode('sessions))
      .drop('sessions)
      .withColumn("sessionLength", sessionLength('session))

    def extractTimestamp(index: Int): UserDefinedFunction = udf[Timestamp, Row] { row: Row =>
      row.getTimestamp(index)
    }

    val extractTracks = udf[String, Row] { row: Row =>
      row.get(SessionFields.SessionTracksIndex).toString.replaceFirst("WrappedArray", "")
    }

    sessionsWithLength.sort('sessionLength.desc)
      .limit(args.maxRank)
      .withColumn("start", extractTimestamp(SessionFields.SessionStartIndex)('session))
      .withColumn("end", extractTimestamp(SessionFields.SessionEndIndex)('session))
      .withColumn("tracks", extractTracks('session))
      .select('userId, 'start, 'end, 'tracks)
      .as[(String, Timestamp, Timestamp, String)]
  }

  override def parseArgs(argsArray: Array[String]): MaxRankArgs =
    MaxRankArgs(argsArray)


  run(args)

}