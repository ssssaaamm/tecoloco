package control

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}

object ProcessInvalidData {

  /**
   * Function that handles the corrupted records. Stores the corrupted records in the garbage table
   * @param data: the corrupted records
   * @param config: configuration file
   */
  def apply(data: DataFrame, config: Config) = {
    val env = config.getString("env")
    val garbagePath: String = if(env=="cloud") config.getString("cloudGarbagePath") else config.getString("localGarbagePath")

    // writing into garbage table.
    data
      .select("invalid_data")
      .withColumn("processed_time", current_timestamp())
      .withColumn("process_name", lit(config.getString("dataWarehousePipeLineName")))
      .write.mode("append").parquet(garbagePath)
  }
}
