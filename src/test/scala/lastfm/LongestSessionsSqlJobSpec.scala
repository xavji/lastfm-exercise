package lastfm

import org.scalatest.LoneElement

class LongestSessionsSqlJobSpec extends SparkSpec with LongestSessionsFixture with TsvFixture with LoneElement {

  "LongestSessionsSqlJob" should "find the first two longest sessions" in { f =>
    val tracks = dataSet(LongestSessionsTracks, f.sparkSession)

    LongestSessionsSqlJob.processTracks(tracks, f.sparkSession, MaxRankArgs("", "", 2))
      .collect() should contain theSameElementsAs ExpectedTop2Sessions
  }

  it should "read a TSV file with tracks and store a CSV file with the longest sessions" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    LongestSessionsSqlJob.run(Array(SampleFilePath, outputDir.toString, "1"))

    verifyLongestSessionsCsv(outputDir)
  }

}