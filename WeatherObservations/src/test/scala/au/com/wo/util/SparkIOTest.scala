package au.com.wo.util

import au.com.wo.SparkSessionProvider

class SparkIOTest extends SparkSessionProvider {

  "The functin readLogFile" should {
    "return a dataset of BaseLog based on what is in the the file" in {
      // GIVEN
      val filePath = "./test.log"

      // WHEN
      val result = SparkIO.readLogFile(filePath)

      // THEN
      result.count() must_== 3
    }
  }
}
