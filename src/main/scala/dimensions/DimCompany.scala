package dimensions

import com.typesafe.config.Config
import common.Utility.getDataWarehousePath
import common.{Schema, Utility}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimCompany {

  val dimDir: String = "/dim_company"

  /**
   * Build and load the dimension dataframe in data warehouse location (s3 bucket)
   * @param data: data extracted from data lake (raw data)
   * @param config: configuration file
   * @param spark: spark session for this app
   */
  def loadDimension(data: DataFrame, config: Config, spark: SparkSession): Unit = {

    val newData = data
      .select("company","sector", "city")
      .withColumn("company", coalesce(col("company"), lit("Unknown")))
      .withColumn("sector", coalesce(col("sector"), lit("Unknown")))
      .withColumn("city", coalesce(col("city"), lit("Unknown")))
      .distinct()

    val existingData = readDimension(config, spark)
      .drop("id_dim_company")

    val finalDimDf = newData
      .unionByName(existingData)
      .groupBy("company", "sector", "city").count()
      .filter(col("count") === 1)
      .withColumn("id_dim_company", expr("uuid()"))

      .select("id_dim_company", "company", "sector", "city")

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
    spark.read.schema(Schema.dimCompanySchema).parquet(dimPath)
  }

}
