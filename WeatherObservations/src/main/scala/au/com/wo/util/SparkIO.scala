package au.com.wo.util

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}

object SparkIO {
  val options = Map(("delimiter", "|"),
    ("header", "true"),
    ("mode", "FAILFAST"))

  def readLogFile(filePath: String)(implicit sparkSession: SparkSession): sql.DataFrame = {
    sparkSession.read
      .options(options)
      .csv(filePath)
  }
}
