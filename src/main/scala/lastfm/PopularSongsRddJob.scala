package lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PopularSongsRddJob extends App with RddJobTemplate[MaxRankArgs, (String, String, Long)] {

  override def jobName: String = "PopularSongsRddJob"

  override def processTracks(tracks: RDD[Track], sparkContext: SparkContext, args: MaxRankArgs): RDD[(String, String, Long)] = {
    val orderByCount = new Ordering[(String, String, Long)] {
      override def compare(x: (String, String, Long), y: (String, String, Long)): Int = {
        x._3.compareTo(y._3)
      }
    }
    val results = tracks.map(t => (t.artistName, t.trackName) -> 1L)
      .reduceByKey(_ + _)
      .map { case ((artist, track), count) => (artist, track, count) }
      .top(args.maxRank)(orderByCount)
    sparkContext.parallelize(results.toList)
  }

  override def parseArgs(argsArray: Array[String]): MaxRankArgs =
    MaxRankArgs(argsArray)

  def run(argsArray: Array[String]): Unit = {
    run(argsArray, PopularFunctions.rowToCsvLine)
  }

  run(args)

}

object PopularFunctions {
  val rowToCsvLine: ((String, String, Long)) => String =
    row => s"${row._1},${row._2},${row._3}"
}