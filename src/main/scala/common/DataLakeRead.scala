package common

import java.sql.Timestamp

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object DataLakeRead {

  /**
   * Function that extract incrementally the data from the datalake (s3 folder) and returns it as a dataframe
   * @param config: configuration file
   * @param spark: SparkSession of this spark application
   * @return: a dataframe with the data lake data
   */
  def apply(config: Config, spark: SparkSession): (DataFrame, DataFrame)= {
    // Extract the data from the datalake

    val env: String = config.getString("env")
    val (dataLakePath: String,  controlPath: String) = if(env=="cloud"){
      (config.getString("cloudDataLakePath"), config.getString("cloudControlPath"))
    } else {
      (config.getString("localDataLakePath"), config.getString("localControlPath"))
    }

    // reading control table for incremental data read
    val lastDataDate: Timestamp = Try(
      spark.read.csv(controlPath)
      .filter(col("process_name")===config.getString("dataWarehousePipeLineName"))
      .agg(max(col("last_data")))
      .collect()
      .map(r => r.getTimestamp(0))
      .last
    ) match {
      case Success(x) => x
      case Failure(x) => Timestamp.valueOf("1900-01-01 00:00:00") //default date (for initial load)
    }

    // Incremental data read
    val data = spark.read.json(dataLakePath)
      .withColumn("file_modification_time", col("_metadata.file_modification_time"))
      .filter(col("file_modification_time") > lastDataDate)

    val validRecords = data.filter("_corrupt_record is null").drop("_corrupt_record")
      .cache() //Todo: Evaluate if it is necessary to write temporarily

    val invalidRecords = data.filter("_corrupt_record is not null")
      .withColumn("invalid_data", col("_corrupt_record"))
      .cache()

    (validRecords, invalidRecords)
  }

}
