package dimensions

import com.typesafe.config.Config
import common.Utility.getDataWarehousePath
import common.{Schema, Utility}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimJobInfo {

  val dimDir: String = "/dim_job_info"

  /**
   * Build and load the dimension dataframe in data warehouse location (s3 bucket)
   * @param data: data extracted from data lake (raw data)
   * @param config: configuration file
   * @param spark: spark session for this app
   */
  def loadDimension(data: DataFrame, config: Config, spark: SparkSession): Unit = {

    val newData = data
      .select("title","benefits")
      .withColumnRenamed("title", "job_title")
      .withColumn("job_title", coalesce(col("job_title"), lit("Unknown")))
      .withColumn("benefits", array_sort(col("benefits")))
      .distinct()

    val existingData = readDimension(config, spark)
      .drop("id_dim_job_info")

    val finalDimDf = newData
      .unionByName(existingData)
      .groupBy("job_title", "benefits").count()
      .filter(col("count") === 1)
      .withColumn("id_dim_job_info", expr("uuid()"))
      .select("id_dim_job_info", "job_title", "benefits")

    val dimPath = getDataWarehousePath(config) + dimDir

    if(finalDimDf.take(1).size > 0)
      finalDimDf.write.mode("append").parquet(dimPath)
  }


  /**
   * Read the information of the dimension from the data warehouse
   * @param config: configuration file
   * @param spark: spark session for this app
   * @return: Dataframe with the dimension information
   */
  def readDimension(config: Config, spark: SparkSession): DataFrame = {
    val dimPath = Utility.getDataWarehousePath(config) + dimDir
    spark.read.schema(Schema.dimJobInfoSchema).parquet(dimPath)
  }

}
