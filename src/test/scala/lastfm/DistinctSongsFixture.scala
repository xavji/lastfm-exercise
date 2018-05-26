package lastfm

import java.sql.Timestamp

import scala.collection.immutable.Seq

trait DistinctSongsFixture {

  val DistinctSongsCountTracks = Seq(
    Track("user_000001", new Timestamp(1L), "Rocket Empire", "Simmer Down Jammie"),
    Track("user_000001", new Timestamp(2L), "Rocket Empire", "Simmer Down Jammie"),
    Track("user_000001", new Timestamp(3L), "Bombay Bicycle Club", "You Already Know (Feat. Kathryn Williams)"),
    Track("user_000002", new Timestamp(2L), "Some artist", "Strings attached")
  )

  val ExpectedCounts = Seq(
    "user_000001" -> 2,
    "user_000002" -> 1
  )

}