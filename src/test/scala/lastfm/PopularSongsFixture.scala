package lastfm

import java.sql.Timestamp

import scala.collection.immutable.Seq

trait PopularSongsFixture {

  val PopularSongsTracks = Seq(
    Track("user_000001", new Timestamp(1L), "Artist 1", "Song 1"),
    Track("user_000002", new Timestamp(2L), "Artist 2", "Song 2"),
    Track("user_000003", new Timestamp(3L), "Artist 3", "Song 3"),
    Track("user_000001", new Timestamp(4L), "Artist 1", "Song 1"),
    Track("user_000002", new Timestamp(5L), "Artist 2", "Song 2"),
    Track("user_000004", new Timestamp(6L), "Artist 1", "Song 1")
  )

  val ExpectedTop2Popularity = Seq(
    ("Artist 1", "Song 1", 3),
    ("Artist 2", "Song 2", 2)
  )

}