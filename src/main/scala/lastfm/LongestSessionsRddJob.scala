package lastfm

import java.sql.Timestamp

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.concurrent.duration._

case class Session(userId: String, start: Timestamp, end: Timestamp,
                   tracks: mutable.Buffer[(String, String)] = mutable.ArrayBuffer.empty[(String, String)]) {

  lazy val durationInMs: Long =
    end.getTime - start.getTime

  def asTuple: (String, Timestamp, Timestamp, String) =
    (userId, start, end, tracks.map { case (art, trk) => s"[$art,$trk]" }.mkString("(", ", ", ")"))

  override def toString: String =
    s"Session($userId, $start, $end, ${tracks.mkString("|")}"

}


object LongestSessionsRddJob extends App with RddJobTemplate[MaxRankArgs, (String, Timestamp, Timestamp, String)]
  with Logging {

  import LongestSessionsOps._

  type Sessions = mutable.ArrayBuffer[Session]

  override def jobName: String = "LongestSessionsRddJob"

  override def processTracks(tracks: RDD[Track], sparkContext: SparkContext, args: MaxRankArgs): RDD[(String, Timestamp, Timestamp, String)] = {
    val sessions: RDD[Session] =
      tracks.groupBy(_.userId)
        .aggregateByKey(mutable.ArrayBuffer.empty[Session])(appendSingleUserTracks, combineSessions)
        .flatMap { case (_, sessions) => sessions }

    val orderByDuration = new Ordering[Session] {
      override def compare(left: Session, right: Session): Int = {
        left.durationInMs.compareTo(right.durationInMs)
      }
    }
    val topSessions = sessions.top(args.maxRank)(orderByDuration)
      .map(_.asTuple)
    sparkContext.parallelize(topSessions)
  }

  private implicit val timestampOrdering = new Ordering[Timestamp] {
    override def compare(left: Timestamp, right: Timestamp): Int =
      left.getTime.compareTo(right.getTime)
  }

  private def appendSingleUserTracks(sessions: Sessions, tracks: Iterable[Track]): Sessions = {
    val currentUserSessions = mutable.ArrayBuffer.empty[Session]

    def addSession(track: Track): Unit = {
      currentUserSessions += Session(track.userId, track.playedOn, track.playedOn, mutable.ArrayBuffer(track.artistName -> track.trackName))
    }

    tracks.toArray.sortBy(_.playedOn.getTime) foreach { track =>
      logger.debug(s"processing $track")
      currentUserSessions.lastOption map { previousSession =>
        if (track.playedOn.inSameSession(previousSession.end)) {
          logger.debug(s"$track in the same session as $previousSession")
          previousSession.tracks += track.artistName -> track.trackName
          val newSession = previousSession.copy(end = track.playedOn)
          logger.debug(s"replacing session $newSession")
          currentUserSessions.update(currentUserSessions.size - 1, newSession)
        } else addSession(track)
      } getOrElse {
        logger.debug(s"no sessions, creating one")
        addSession(track)
      }
    }
    sessions ++= currentUserSessions
    sessions
  }

  private def combineSessions(s1: Sessions, s2: Sessions): Sessions = {
    s1 ++= s2
    s1
  }


  override def parseArgs(argsArray: Array[String]): MaxRankArgs =
    MaxRankArgs(argsArray)

  def run(argsArray: Array[String]): Unit = {
    run(argsArray, rowToCsvLine)
  }

  run(args)

}

object LongestSessionsOps {

  val SessionWindowInMs = 20.minutes.toMillis

  val rowToCsvLine: ((String, Timestamp, Timestamp, String)) => String =
    row => s"${row._1},${row._2},${row._3},${row._4}"

  implicit class RichTimestamp(ts: Timestamp) {
    def inSameSession(otherTs: Timestamp): Boolean = {
      ts.getTime - otherTs.getTime <= SessionWindowInMs
    }
  }

}