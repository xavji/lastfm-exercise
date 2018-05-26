package lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DistinctSongsByUserRddJob extends App with RddJobTemplate[DistinctSongsArgs, (String, Long)] {

  override def jobName: String = "DistinctSongsByUserRddJob"

  override def parseArgs(argsArray: Array[String]): DistinctSongsArgs =
    DistinctSongsArgs(argsArray)


  override def processTracks(tracks: RDD[Track], sparkContext: SparkContext, args: DistinctSongsArgs): RDD[(String, Long)] = {
    tracks.map(t => (t.userId, t.artistName, t.trackName))
      .distinct()
      .map { case (userId, _, _) => userId -> 1L }
      .reduceByKey(_ + _)
  }


  def run(argsArray: Array[String]): Unit = {
    run(argsArray, DistinctFunctions.rowToCsvLine)
  }

  run(args)

}

object DistinctFunctions {
  val rowToCsvLine: ((String, Long)) => String =
    row => s"${row._1},${row._2}"
}
