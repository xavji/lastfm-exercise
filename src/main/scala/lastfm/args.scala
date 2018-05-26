package lastfm

sealed trait Args {
  def inputPath: String

  def outputPath: String
}

case class DistinctSongsArgs(inputPath: String, outputPath: String) extends Args

object DistinctSongsArgs {
  def apply(args: Array[String]): DistinctSongsArgs = args match {
    case Array(inputPath, outputPath) => DistinctSongsArgs(inputPath, outputPath)
    case _ => throw new IllegalArgumentException("needs two program arguments for TSV input file path and output file path")
  }
}

case class MaxRankArgs(inputPath: String, outputPath: String, maxRank: Int) extends Args

object MaxRankArgs {
  def apply(args: Array[String]): MaxRankArgs = args match {
    case Array(inputPath, outputPath, maxRank) => MaxRankArgs(inputPath, outputPath, maxRank.toInt)
    case _ => throw new IllegalArgumentException("needs three program arguments for TSV input file path, output file path, and max rank (Int)")
  }
}
