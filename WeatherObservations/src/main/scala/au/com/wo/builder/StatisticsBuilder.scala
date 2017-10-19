package au.com.wo.builder

import au.com.wo.builder.StatisticsBuilder.{countField, observatoryField, temperatureField}
import au.com.wo.model.{Log, LogStatistics}
import org.apache.spark.sql._

object StatisticsBuilder {
  val dateFormat = "yyyy-MM-dd'T'HH:mm"
  val temperatureField = "temperature"
  val observatoryField = "observatory"
  val countField = "count"
  val observatories = List("AU", "FR", "BE", "US", "CA", "NZ")
}
class StatisticsBuilder(sparkSession: SparkSession) extends Serializable {

  def execute : (String, (String) => Dataset[Row]) => LogStatistics = (fileName, func) => {

    import sparkSession.implicits._

    val func1 = func(fileName)
    val logDataset = func1.map(transformToLog)

    implicit val spark = sparkSession

    val (max, min, mean) = getTemperatureRange(logDataset)
    val observationsCount = groupByObservatories.andThen(datasetToMap).apply(logDataset)

    LogStatistics(observationsCount, max, min, mean)
  }

  private[builder] def datasetToMap(observatoryDS: RelationalGroupedDataset)(implicit sparkSession: SparkSession): Map[String, Long] = {
    import sparkSession.implicits._
    observatoryDS.count().toDF(observatoryField, countField)
      .map(r => (r.getAs[String](observatoryField), r.getLong(1)))
      .coalesce(1)
      .collect().toMap
  }

  private[builder] def getTemperatureRange(logDataset: Dataset[Log])(implicit sparkSession: SparkSession) : (Double, Double, Double) = {
    import sparkSession.implicits._
    val temperatures = logDataset
      .map(log => log.temperature)
      .collect()
      .toSeq

    (temperatures.max, temperatures.min, temperatures.sum / temperatures.length)
  }

  private[builder] val groupByObservatories = (logDataset: Dataset[Log]) => {
    logDataset.groupBy(observatoryField)
  }

  private[builder] val transformToLog: (Row) => Log = row => {
    val date = row.getAs[String]("timestamp")
    val location = row.getAs[String]("location").split(";")

    val (longitude, latitude) = location match {
      case _ if location.length > 1 => (location(0).toDouble, location(1).toDouble)
      case _ => (0.0, 0.0)
    }
    val observatory = row.getAs[String](observatoryField)
    val temperature = getCelciusDegrees(observatory, row.getAs[String](temperatureField).toDouble)

    Log(date, longitude, latitude, temperature, observatory)
  }

  private val getCelciusDegrees = (observatory: String, temp: Double) => {
    observatory match {
      case "US" => (temp - 32) / 2
      case "AU" => temp
      case _ => temp - 273.15
    }
  }
}