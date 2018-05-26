package lastfm

import org.apache.logging.log4j.scala.Logging

object Timer extends Logging {

  private val MsPerSecond = 1000

  def timed[T](f: => T): T = {
    val start = System.currentTimeMillis()
    try {
      f
    } finally {
      val ellapsed = System.currentTimeMillis() - start
      logger.info(s"execution time ${ellapsed / MsPerSecond} seconds")
    }
  }
}
