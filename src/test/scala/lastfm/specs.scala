package lastfm

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{Matchers, Outcome, fixture}

trait SparkFixture extends fixture.FlatSpecLike {

  case class Fixture(sparkSession: SparkSession, tmpDir: Path)

  type FixtureParam = Fixture

  def withFixture(test: OneArgTest): Outcome = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getSimpleName)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val tmpDir = Files.createTempDirectory(getClass.getSimpleName)
    try {
      test(Fixture(sparkSession, tmpDir))
    } finally FileUtils.forceDelete(tmpDir.toFile)
  }

  def dataSet(tracks: Seq[Track], session: SparkSession): Dataset[Track] = {
    import session.implicits._
    session.sparkContext.parallelize(tracks).toDS()
  }
}

abstract class SparkSpec extends fixture.FlatSpec with SparkFixture with Matchers