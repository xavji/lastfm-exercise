package lastfm

class PopularSongsSqlJobSpec extends SparkSpec with PopularSongsFixture with TsvFixture {

  "PopularSongsSqlJob" should "rank tracks played and keep the top N" in { f =>
    val tracks = dataSet(PopularSongsTracks, f.sparkSession)

    PopularSongsSqlJob.processTracks(tracks, f.sparkSession, MaxRankArgs("", "", 2))
      .collect() should contain theSameElementsAs ExpectedTop2Popularity
  }

  it should "read a TSV file with tracks and store a CSV file with popularity" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    PopularSongsSqlJob.run(Array(SampleFilePath, outputDir.toString, "1"))

    verifyCsv(outputDir, ExpectedCsvLinesPopularSongs)
  }

}