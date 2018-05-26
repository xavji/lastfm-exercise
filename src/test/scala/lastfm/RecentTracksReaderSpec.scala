package lastfm

class RecentTracksReaderSpec extends SparkSpec with TsvFixture {

  "RecentTracksReader" should "parse a small extract of the real data as a Dataset[Track]" in { f =>
    RecentTracksReader.recentTracksDataset(f.sparkSession, SampleFilePath)
      .collect() should contain theSameElementsAs ExpectedTracks
  }

  it should "parse a small extract of the real data as an RDD[Track]" in { f =>
    RecentTracksReader.recentTracksRDD(f.sparkSession.sparkContext, SampleFilePath)
      .collect() should contain theSameElementsAs ExpectedTracks
  }

}