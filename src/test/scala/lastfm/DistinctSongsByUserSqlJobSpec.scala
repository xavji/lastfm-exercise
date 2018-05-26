package lastfm

class DistinctSongsByUserSqlJobSpec extends SparkSpec with DistinctSongsFixture with TsvFixture {

  "DistinctSongsByUserSqlJob" should "count distinct tracks played by each user" in { f =>
    val tracks = dataSet(DistinctSongsCountTracks, f.sparkSession)

    DistinctSongsByUserSqlJob.processTracks(tracks, f.sparkSession, DistinctSongsArgs("not used here", "not used here"))
      .collect() should contain theSameElementsAs ExpectedCounts
  }

  it should "read a TSV file with tracks and store a CSV file with counts" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    DistinctSongsByUserSqlJob.run(Array(SampleFilePath, outputDir.toString))

    verifyCsv(outputDir, ExpectedCsvLinesDistinctSongs)
  }

}