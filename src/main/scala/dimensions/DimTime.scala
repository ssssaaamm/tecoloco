package dimensions

import com.typesafe.config.{Config, ConfigFactory}
import common.Utility
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimTime {

  val dimDir: String = "/dim_time"

  /**
   * Application created to load (one single time) the time dimension.
   * It generates a series of dates and build the time dimension dataframe.
   * It also stores that information in the time data warehouse dimension.
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    val spark = Utility.getSparkSession(config)

    val df = spark.sql("SELECT sequence(to_date('2009-12-31'), to_date('2021-01-01'), interval 1 day) as date")
      .withColumn("date",explode(col("date")))
      .withColumn("id_dim_time", date_format(col("date"), "yyyyMMdd").cast(StringType))
      .withColumn("year", year(col("date")).cast(IntegerType))
      .withColumn("month", month(col("date")).cast(IntegerType))
      .withColumn("day", dayofmonth(col("date")).cast(IntegerType))
      .select("id_dim_time","date", "year", "month", "day")

    val dimTimePath = Utility.getDataWarehousePath(config) + dimDir

    df.write.mode("append").parquet(dimTimePath)
  }


  /**
   * Read the information of the dimension from the data warehouse
   * @param config: configuration file
   * @param spark: spark session for this app
   * @return: Dataframe with the dimension information
   */
  def readDimension(config: Config, spark: SparkSession): DataFrame = {
    val dimPath = Utility.getDataWarehousePath(config) + dimDir
    spark.read.parquet(dimPath)
  }

}
