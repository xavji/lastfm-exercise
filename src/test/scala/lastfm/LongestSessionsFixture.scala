package lastfm

import java.sql.Timestamp

import scala.collection.immutable.Seq

trait LongestSessionsFixture {

  import TimestampOps._

  private val twentyMinutesInMs = 1200000
  private val user1SessionStart = ts(11200001L)
  private val user1SessionEnd = user1SessionStart + (2 * twentyMinutesInMs - 23001)
  private val user5SessionStart = ts(50000000L)
  private val user5SessionEnd = user5SessionStart + 2800000

  val LongestSessionsTracks = Seq(
    Track("user_1", ts(10000000L), "Artist 1", "Song 1"),
    Track("user_2", ts(2L), "Artist 2", "Song 2"),
    Track("user_3", ts(3L), "Artist 3", "Song 3"),
    Track("user_1", user1SessionStart, "Artist 1", "Song 2"), // not in same session as line 18
    Track("user_2", ts(5L), "Artist 2", "Song 2"),
    Track("user_1", user1SessionStart + twentyMinutesInMs, "Artist 1", "Song 3"), // in same session as line 11
    Track("user_4", ts(6L), "Artist 4", "Song 1"),
    Track("user_1", user1SessionEnd, "Artist 1", "Song 4"), // in same session as line 23
    Track("user_5", user5SessionStart, "Artist 5", "Song 1"),
    Track("user_1", ts(18100000L), "Artist 1", "Song 5"), // not in same session as line 25
    Track("user_5", user5SessionStart + 800000, "Artist 6", "Song 8"), // in same session as line 26
    Track("user_4", ts(7L), "Artist 4", "Song 9"),
    Track("user_5", user5SessionStart + 1800000L, "Artist 7", "Song 3"), // in same session as line 28
    Track("user_3", ts(8L), "Artist 3", "Song 3"),
    Track("user_5", user5SessionEnd, "Artist 3", "Song 15"), // in same session as line 30
    Track("user_2", ts(9L), "Artist 2", "Song 2"),
    Track("user_5", ts(60000000L), "Artist 1", "Song 6"), // not in same session as line 32
    Track("user_4", ts(10L), "Artist 2", "Song 2"),
    Track("user_5", ts(60000000L) + 547000, "Artist 9", "Song 5"), // in same session as line 34
    Track("user_4", ts(11L), "Artist 3", "Song 14")
  )

  private val User1SessionArtists = "([Artist 1,Song 2], [Artist 1,Song 3], [Artist 1,Song 4])"

  private val User5SessionArtists = "([Artist 5,Song 1], [Artist 6,Song 8], [Artist 7,Song 3], [Artist 3,Song 15])"

  val ExpectedTop2Sessions = Set(
    ("user_1", user1SessionStart, user1SessionEnd, User1SessionArtists),
    ("user_5", user5SessionStart, user5SessionEnd, User5SessionArtists)
  )

  def ts(l: Long): Timestamp = new Timestamp(l)

}

object TimestampOps {

  implicit class RichTimestamp(ts: Timestamp) {
    def +(millis: Long): Timestamp =
      new Timestamp(ts.getTime + millis)
  }

}