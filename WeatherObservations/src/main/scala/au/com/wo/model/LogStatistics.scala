package au.com.wo.model

case class LogStatistics(observatories: Map[String, Long], maxTemperature: Double, minTemperature: Double, meanTemperature: Double)
