package lastfm

class LongestSessionsRddJobSpec extends SparkSpec with LongestSessionsFixture with TsvFixture {

  "LongestSessionsRddJob" should "find the first two longest sessions" in { f =>
    val tracks = f.sparkSession.sparkContext.parallelize(LongestSessionsTracks)

    LongestSessionsRddJob.processTracks(tracks, f.sparkSession.sparkContext, MaxRankArgs("", "", 2))
      .collect() should contain theSameElementsAs ExpectedTop2Sessions
  }

  it should "read a TSV file with tracks and store a CSV file with the longest sessions" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    LongestSessionsRddJob.run(Array(SampleFilePath, outputDir.toString, "1"))

    verifyLongestSessionsCsv(outputDir)
  }

}