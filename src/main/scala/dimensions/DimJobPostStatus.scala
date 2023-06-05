package dimensions

import com.typesafe.config.Config
import common.Utility.getDataWarehousePath
import common.{Schema, Utility}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimJobPostStatus {

  val dimDir: String = "/dim_job_post_status"

  /**
   * Build and load the dimension dataframe in data warehouse location (s3 bucket)
   * @param data: data extracted from data lake (raw data)
   * @param config: configuration file
   * @param spark: spark session for this app
   */
  def loadDimension(data: DataFrame, config: Config, spark: SparkSession): Unit = {

    val newData = data
      .select(col("adverts.status").as("post_status"))
      .distinct()

    val existingData = readDimension(config, spark)
      .drop("id_dim_job_post_status")

    val finalDimDf = newData
      .unionByName(existingData)
      .groupBy("post_status").count()
      .filter(col("count") === 1)
      .withColumn("id_dim_job_post_status", expr("uuid()"))
      .select("id_dim_job_post_status", "post_status")

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
    spark.read.schema(Schema.dimJobPostStatusSchema).parquet(dimPath)
  }

}
