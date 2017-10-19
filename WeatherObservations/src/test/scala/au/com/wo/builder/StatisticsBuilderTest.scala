package au.com.wo.builder

import au.com.wo.SparkSessionProvider
import au.com.wo.model.Log
import au.com.wo.util.SparkIO
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class StatisticsBuilderTest extends SparkSessionProvider {
  val statisticBuilder = new StatisticsBuilder(sparkSession)

  val columns = List("timestamp", "location", "temperature", "observatory")
  val schema = new StructType(columns.map(c => new StructField(c, StringType, false)).toArray)

  "The function execute" should {
    "return statistics on the batch of logs" in {
      val statistics = statisticBuilder.execute("test.log", SparkIO.readLogFile)

      statistics.observatories must_== Map("NZ" -> 1, "CA" -> 2)
    }
  }
  "The function transformToLog" should {
    "transform a Row into a Log" in {
      "and convert properly in celcius degrees the temperature in the default case" in {
        // GIVEN
        val row0 = Array[Any]("2017-10-16T05:45", "-179.6;50.09", "-11.37", "NZ")
        val row = new GenericRowWithSchema(row0, schema)

        // WHEN
        val result = statisticBuilder.transformToLog(row)

        // THEN
        result.observatory must_== "NZ"
        result.longitude must_== -179.6
        result.latitude must_== 50.09
        result.temperature must_== -284.52
        result.date must_== "2017-10-16T05:45"
      }
      "and convert properly in celcius degrees the temperature in the AU case" in {
        // GIVEN
        val rowBenefitRights = Array[Any]("2017-10-16T05:45", "-179.6;50.09", "-11.37", "AU")
        val row = new GenericRowWithSchema(rowBenefitRights, schema)

        // WHEN
        val result = statisticBuilder.transformToLog(row)

        // THEN
        result.observatory must_== "AU"
        result.longitude must_== -179.6
        result.latitude must_== 50.09
        result.temperature must_== -11.37
        result.date must_== "2017-10-16T05:45"
      }
      "and convert properly in celcius degrees the temperature in the US case" in {
        // GIVEN
        val rowBenefitRights = Array[Any]("2017-10-16T05:45", "-179.6;50.09", "72", "US")
        val row = new GenericRowWithSchema(rowBenefitRights, schema)

        // WHEN
        val result = statisticBuilder.transformToLog(row)

        // THEN
        result.observatory must_== "US"
        result.longitude must_== -179.6
        result.latitude must_== 50.09
        result.temperature must_== 20
        result.date must_== "2017-10-16T05:45"
      }
    }
  }
  "The function getTemperatureRange" should {
    "return max, min and mean temperatures of the log file" in {
      // GIVEN
      val log1 = Log("2017-10-16T05:45", 10.0, 10.0, 12, "CA")
      val log2 = Log("2017-10-17T05:45", 10.0, 10.0, 13, "CA")
      val log3 = Log("2017-10-18T05:45", 10.0, 10.0, 14, "CA")

      import sparkSession.implicits._

      val logDataset = sparkSession.createDataset(Seq(log1, log2, log3))

      // WHEN
      val (max, min, mean) = statisticBuilder.getTemperatureRange(logDataset)

      // THEN
      max must_== 14
      min must_== 12
      mean must_== 13
    }
  }


//  val func : (String) => Dataset[Row] = str => {
//    val row0 = Array[Any]("2017-10-16T05:45", "-179.6,50.09", "5", "NZ")
//    val row1 = Array[Any]("2017-10-17T05:45", "-179.6,50.09", "10", "NZ")
//    val row2 = Array[Any]("2017-10-18T05:45", "-179.6,50.09", "10", "NZ")
//    val row3 = Array[Any]("2017-10-16T05:45", "-179.6,50.09", "15", "CA")
//    val row4 = Array[Any]("2017-10-17T05:45", "-179.6,50.09", "10", "CA")
//    val genericRow = new GenericRowWithSchema(row0, schema)
//    val genericRow1 = new GenericRowWithSchema(row1, schema)
//    val genericRow2 = new GenericRowWithSchema(row2, schema)
//    val genericRow3 = new GenericRowWithSchema(row3, schema)
//    val genericRow4 = new GenericRowWithSchema(row4, schema)
//
//    import sparkSession.implicits._
//    val ds = sparkSession.emptyDataset[Row]
//    ds
//  }
}
