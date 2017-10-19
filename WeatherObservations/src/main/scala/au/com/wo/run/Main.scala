package au.com.wo.run

import au.com.wo.builder.StatisticsBuilder
import au.com.wo.util.SparkIO
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {
  val appName = "Weather Observations - Technical test"
  val logger = LoggerFactory.getLogger(Main.getClass)
  val weatherObservationLogs = "./weather-observations.log"

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    implicit val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName(appName)
      .getOrCreate()

    val builder = new StatisticsBuilder(sparkSession)

    val statistics = builder.execute(weatherObservationLogs, SparkIO.readLogFile)

    logger.info("######### start Statistics #########\n")
    logger.info(statistics.toString)

    val endTime = System.currentTimeMillis()

    logger.info(s"######### Time taken : ${(endTime-startTime)/1000} seconds\n")
  }
}
