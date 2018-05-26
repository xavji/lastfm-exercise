package lastfm

import java.nio.file.Path
import java.sql.Timestamp

import org.scalatest.{LoneElement, Matchers}

import scala.io.Source

trait TsvFixture extends LoneElement {
  this: Matchers =>

  val SampleFilePath = "src/test/resources/tracks-sample.tsv"

  val ExpectedTracks = Set(
    Track("user_000001", new Timestamp(1241444531000L), "坂本龍一", "Mc1 (Live_2009_4_15)"),
    Track("user_000001", new Timestamp(1241443964000L), "The Young Lovers", "How Lonely Does It Get"),
    Track("user_000001", new Timestamp(1241274656000L), "Rocket Empire", "Simmer Down Jammie"),
    Track("user_000002", new Timestamp(1241357564000L), "The Young Lovers", "How Lonely Does It Get")
  )

  val ExpectedCsvLinesDistinctSongs = Set("user_000001,3", "user_000002,1")

  val ExpectedCsvLinesPopularSongs = Set("The Young Lovers,How Lonely Does It Get,2")

  val ExpectedCsvLinesLongestSessions = Set("user_000001,2009-05-04T14:32:44.000+01:00,2009-05-04T14:42:11.000+01:00,\"[[The Young Lovers, How Lonely Does It Get], [坂本龍一, Mc1 (Live_2009_4_15)]]\"")

  def verifyCsv(tmpDir: Path, linesAssertion: List[String] => Unit): Unit = {
    val allCsvLines = tmpDir.toFile.listFiles().collect {
      case f if f.getName.startsWith("part-") => Source.fromFile(f).getLines().toList
    }.flatten.toList
    linesAssertion(allCsvLines)
  }

  def verifyCsv(tmpDir: Path, expectedLines: Set[String]): Unit =
    verifyCsv(tmpDir, _ should contain theSameElementsAs expectedLines)


  def verifyLongestSessionsCsv(tmpDir: Path): Unit = {
    verifyCsv(tmpDir, csvLinesAssertions _)
  }

  def csvLinesAssertions(lines: List[String]): Unit = {
    // Regex because the timestamps in the CSV use the current locale to express the time element
    // this regex would not work for time zones which are not integer offsets in hours from UTC
    lines.loneElement should fullyMatch regex """user_000001,2009-05-04[T ]\d{2}:32:44.+?,2009-05-04[T ]\d{2}:42:11.+?,"?\(\[The Young Lovers,How Lonely Does It Get\], \[坂本龍一,Mc1 \(Live_2009_4_15\)\]\)"?"""
  }

}