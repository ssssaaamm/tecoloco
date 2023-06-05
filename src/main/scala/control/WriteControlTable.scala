package control

import com.typesafe.config.Config
import common.Utility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, max}

object WriteControlTable {

  /**
   * Write into control table. This table is used to perform incremental reads
   * @param data: data that was loaded in the datalake
   * @param config: configuration file
   */
  def apply(data: DataFrame, config: Config): Unit = {
    val controlPath: String = Utility.getControlPath(config)

    // writing into control table.
    data.select( "file_modification_time" )
      .agg(max("file_modification_time").as("last_data"))
      .withColumn("process_name", lit(config.getString("dataWarehousePipeLineName")))
      .withColumn("runtime", current_timestamp())
      .select( "process_name","runtime","last_data")
      .write.mode("append").parquet(controlPath)
  }

}
