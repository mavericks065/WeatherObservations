package au.com.wo

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

trait SparkSessionProvider extends Specification with AfterAll {

  val logger = LoggerFactory.getLogger(classOf[SparkSessionProvider])

  logger.info("Creating SparkSession for tests")

  implicit val sparkSession = SparkSession
    .builder()
    .appName(getClass.getName)
    .master("local[*]")
    .getOrCreate()

  def afterAll = {
    sparkSession.stop()
    logger.info("Tests SparkSession stopped")
  }

}
