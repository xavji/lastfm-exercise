package lastfm

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

object RecentTracksReader extends Logging {

  private val CsvFields = List(
    StructField("userId", StringType),
    StructField("playedOn", TimestampType),
    StructField("artistId", StringType, nullable = true),
    StructField("artistName", StringType),
    StructField("trackId", StringType, nullable = true),
    StructField("trackName", StringType)
  )

  private val CsvSchema = StructType(CsvFields)

  private val TimestampFormat = "yyyy-MM-dd'T'HH:mm:ssX"

  private val ColumnSeparator = "\t"

  def recentTracksDataset(sparkSession: SparkSession, path: String): Dataset[Track] = {
    import sparkSession.implicits._
    sparkSession.read
      .option("sep", ColumnSeparator)
      .option("timestampFormat", TimestampFormat)
      .schema(CsvSchema)
      .csv(path)
      .as[Track]
  }

  def recentTracksRDD(sparkContext: SparkContext, path: String): RDD[Track] = {
    sparkContext.textFile(path).flatMap(parseTsvRow)
  }

  private def parseTsvRow(row: String): Option[Track] = {
    row.split(ColumnSeparator) match {
      case Array(userId, playedOn, _, artistName, _, trackName) =>
        parseTimestamp(playedOn) map { ts =>
          Track(userId, ts, artistName, trackName)
        }
      case x =>
        logger.warn(s"invalid TSV data '$x'")
        None
    }

  }

  private def parseTimestamp(dateTime: String): Option[Timestamp] = {
    val sdf = new SimpleDateFormat(TimestampFormat)
    Try(sdf.parse(dateTime))
      .map(d => new Timestamp(d.getTime))
      .toOption
  }

}
