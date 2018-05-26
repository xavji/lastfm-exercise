package lastfm

import java.sql.Timestamp

case class Track(userId: String, playedOn: Timestamp, artistName: String, trackName: String)