package lastfm

import java.sql.Timestamp

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable

object SessionFields {
  val SessionUserIdIndex = 0
  val SessionStartIndex = 1
  val SessionEndIndex = 2
  val SessionTracksIndex = 3
}

class SessionFromTracksUdaf extends UserDefinedAggregateFunction with Logging {

  import SessionFields._

  private val userIdIndex = 0
  private val playedOnIndex = 1
  private val endIndex = 2
  private val artistNameIndex = 3
  private val trackNameIndex = 4

  override def inputSchema = StructType(
    List(
      StructField("userId", StringType),
      StructField("playedOn", TimestampType),
      StructField("end", TimestampType),
      StructField("artistName", StringType),
      StructField("trackName", StringType)
    )
  )

  private val sessionTrackType = StructType(
    List(
      StructField("artist", StringType),
      StructField("track", StringType)
    )
  )

  private val sessionType = StructType(
    List(
      StructField("userId", StringType),
      StructField("start", TimestampType),
      StructField("end", TimestampType),
      StructField("tracks", ArrayType(sessionTrackType))
    )
  )

  override def bufferSchema: StructType = StructType(
    List(
      StructField("sessions", DataTypes.createMapType(LongType, sessionType))
    )
  )

  override def dataType: DataType = ArrayType(sessionType)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map.empty[Long, GenericRowWithSchema]
  }

  override def update(buffer: MutableAggregationBuffer, row: Row): Unit = {
    val sessions = buffer.getAs[Map[Long, GenericRowWithSchema]](0)

    logger.debug(s"aggregating $row in $sessions")

    val userId = row.getString(userIdIndex)
    val playedOn = row.getTimestamp(playedOnIndex)
    val end = row.getTimestamp(endIndex)
    val artistName = row.getString(artistNameIndex)
    val trackName = row.getString(trackNameIndex)

    def addSession(session: GenericRowWithSchema): Map[Long, GenericRowWithSchema] = {
      sessions.updated(session.getTimestamp(SessionStartIndex).getTime, session)
        .updated(session.getTimestamp(SessionEndIndex).getTime, session)
    }

    sessions.get(playedOn.getTime) map { previousTracksSession =>
      logger.debug(s"found session before $playedOn")
      val tracks = previousTracksSession.get(SessionTracksIndex).asInstanceOf[mutable.WrappedArray.ofRef[GenericRowWithSchema]]
      val newTracks = tracks :+ newRow(Array(artistName, trackName), sessionTrackType)
      val newSession = newSessionWithTracksAndTimestamp(previousTracksSession, newTracks, end, SessionEndIndex)
      logger.debug(s"new session before $newSession")
      buffer(0) = addSession(newSession) - playedOn.getTime
    } getOrElse {
      if (end != playedOn) { // no need to create a session for isolated tracks
        sessions.get(end.getTime) map { followingTracksSession =>
          logger.debug(s"found session after $end")
          val tracks = followingTracksSession.get(SessionTracksIndex).asInstanceOf[mutable.WrappedArray.ofRef[GenericRowWithSchema]]
          val newTracks = newRow(Array(artistName, trackName), sessionTrackType) +: tracks
          val newSession = newSessionWithTracksAndTimestamp(followingTracksSession, newTracks, playedOn, SessionStartIndex)
          logger.debug(s"new session after $newSession")
          buffer(0) = addSession(newSession) - end.getTime
        } getOrElse {
          logger.debug(s"brand new session for $playedOn and $end")
          val tracks = Array(Row(artistName, trackName))
          buffer(0) = addSession(newRow(Array(userId, playedOn, end, tracks), sessionType))
        }
      } else logger.debug(s"discarding isolated track $row")
    }
  }

  private def newSessionWithTracksAndTimestamp(row: GenericRowWithSchema, tracks: mutable.WrappedArray[GenericRowWithSchema],
                                               timestamp: Timestamp, timestampFieldIndex: Int): GenericRowWithSchema = {
    val userId = row.get(SessionUserIdIndex)
    val start = row.get(SessionStartIndex)
    val end = row.get(SessionEndIndex)
    val values = Array(userId, start, end, tracks)
    values(timestampFieldIndex) = timestamp
    newRow(values, sessionType)
  }

  private def newRow(values: Array[Any], schema: StructType): GenericRowWithSchema =
    new GenericRowWithSchema(values, schema)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val s1 = buffer1.getAs[Map[Long, GenericRowWithSchema]](0)
    val s2 = buffer2.getAs[Map[Long, GenericRowWithSchema]](0)
    buffer1(0) = s1 ++ s2
  }

  override def evaluate(buffer: Row): Any = {
    val sessions = buffer.getAs[Map[Long, GenericRowWithSchema]](0)
    sessions.values.toArray
  }
}