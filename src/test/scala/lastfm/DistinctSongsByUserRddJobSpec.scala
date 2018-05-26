package lastfm

class DistinctSongsByUserRddJobSpec extends SparkSpec with DistinctSongsFixture with TsvFixture {

  "DistinctSongsByUserRddJob" should "count distinct tracks played by each user" in { f =>
    val tracks = f.sparkSession.sparkContext.parallelize(DistinctSongsCountTracks)

    DistinctSongsByUserRddJob.processTracks(tracks, f.sparkSession.sparkContext, DistinctSongsArgs("", ""))
      .collect() should contain theSameElementsAs ExpectedCounts
  }

  it should "read a TSV file with tracks and store a CSV file with counts" in { f =>
    val outputDir = f.tmpDir.resolve("csv-out")

    DistinctSongsByUserRddJob.run(Array(SampleFilePath, outputDir.toString))

    verifyCsv(outputDir, ExpectedCsvLinesDistinctSongs)
  }

}
