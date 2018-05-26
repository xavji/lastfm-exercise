package lastfm

class PopularSongsRddJobSpec extends SparkSpec with PopularSongsFixture with TsvFixture {

  "PopularSongsRddJob" should "rank tracks played and keep the top N" in { f =>
    val tracks = f.sparkSession.sparkContext.parallelize(PopularSongsTracks)

    PopularSongsRddJob.processTracks(tracks, f.sparkSession.sparkContext, MaxRankArgs("", "", 2))
      .collect() should contain theSameElementsAs ExpectedTop2Popularity
  }

  it should "read a TSV file with tracks and store a CSV file with popularity" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    PopularSongsRddJob.run(Array(SampleFilePath, outputDir.toString, "1"))

    verifyCsv(outputDir, ExpectedCsvLinesPopularSongs)
  }

}