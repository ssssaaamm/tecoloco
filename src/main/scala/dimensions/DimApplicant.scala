package dimensions

import com.typesafe.config.Config
import common.Utility.getDataWarehousePath
import common.{Schema, Utility}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimApplicant {

  val dimDir: String = "/dim_applicant"

  /**
   * Build and load the dimension dataframe in data warehouse location (s3 bucket)
   * @param data: data extracted from data lake (raw data)
   * @param config: configuration file
   * @param spark: spark session for this app
   */
  def loadDimension(data: DataFrame, config: Config, spark: SparkSession): Unit = {

    val newData = data
      .select("applicants")
      .withColumn("applicants", explode(col("applicants")))
      .select("applicants.*")
      .select("firstName", "lastName", "age", "skills")
      .where(col("applicationDate").isNotNull)
      .withColumn("first_name", initcap(col("firstName")))
      .withColumn("last_name", initcap(col("lastName")))
      .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
      .withColumn("skills", sort_array(col("skills")))
      .select("first_name", "last_name", "full_name", "age", "skills")
      .distinct()

    val existingData = readDimension(config, spark)
      .drop("id_dim_applicant")

    val finalDimDf = newData
      .unionByName(existingData)
      .groupBy("first_name", "last_name", "full_name", "age", "skills").count()
      .filter(col("count") === 1)
      .withColumn("id_dim_applicant", expr("uuid()"))
      .select("id_dim_applicant", "first_name", "last_name", "full_name", "age", "skills")

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
    spark.read.schema(Schema.dimApplicantSchema).parquet(dimPath)
  }

}
