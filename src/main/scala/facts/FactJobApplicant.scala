package facts

import com.typesafe.config.Config
import common.Utility.getDataWarehousePath
import common.{Schema, Utility}
import dimensions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FactJobApplicant {

  val factDir: String = "/fact_job_applicant"

  /**
   * Build and load the fact table dataframe in data warehouse location (s3 bucket)
   * @param data: data extracted from data lake (raw data)
   * @param config: configuration file
   * @param spark: spark session for this app
   */
  def loadFact(data: DataFrame, config: Config, spark: SparkSession): Unit = {

    val dimCompanyDf = DimCompany.readDimension(config, spark)
    val dimJobInfoDf = DimJobInfo.readDimension(config, spark)
    val dimJobPostStatusDf = DimJobPostStatus.readDimension(config, spark)
    val dimApplicantDf = DimApplicant.readDimension(config, spark)

    val dimCompanyJoinCols = Seq("company", "sector", "city")
    val dimJobInfoJoinCols = Seq("job_title", "benefits")
    val dimJobPostStatusJoinCols = Seq("post_status")
    val dimApplicantJoinCols = Seq("first_name", "last_name", "age", "skills")


    val factDf = data
      .select(
        col("id").as("job_id") //degenerate dimension
        ,coalesce(col("company"), lit("Unknown")).as("company")
        ,coalesce(col("sector"), lit("Unknown")).as("sector")
        ,coalesce(col("city"), lit("Unknown")).as("city")
        ,coalesce(col("title"), lit("Unknown")).as("job_title")
        ,sort_array(col("benefits")).as("benefits")
        ,col("adverts.status").as("post_status")
        ,explode(col("applicants")).as("applicant")
      )
      .select("applicant.*", "*")
      .drop("applicant")
      .where(col("applicationDate").isNotNull)
      .select(
        col("job_id")
        ,date_format(from_unixtime(col("applicationDate")),"yyyyMMdd").as("id_dim_time")
        ,col("company")
        ,col("sector")
        ,col("city")
        ,col("job_title")
        ,col("benefits")
        ,col("post_status")
        ,initcap(col("firstName")).as("first_name")
        ,initcap(col("lastName")).as("last_name")
        ,col("age")
        ,sort_array(col("skills")).as("skills")
      )
      .join(broadcast(dimCompanyDf), dimCompanyJoinCols, "inner")
      .join(broadcast(dimJobInfoDf), dimJobInfoJoinCols, "inner")
      .join(broadcast(dimJobPostStatusDf), dimJobPostStatusJoinCols, "inner")
      .join(broadcast(dimApplicantDf), dimApplicantJoinCols, "inner")
      .withColumn("id_fact_applicant", expr("uuid()"))
      .select("id_fact_applicant","job_id" , "id_dim_time", "id_dim_company", "id_dim_job_info", "id_dim_job_post_status", "id_dim_applicant")

    val factPath = getDataWarehousePath(config) + factDir

    if(factDf.take(1).size > 0)
      factDf.write.mode("append").parquet(factPath)
  }


  /**
   * Read the information of the fact table from the data warehouse
   * @param config: configuration file
   * @param spark: spark session for this app
   * @return: Dataframe with the fact table information
   */
  def readFact(config: Config, spark: SparkSession): DataFrame = {
    val factPath = Utility.getDataWarehousePath(config) + factDir
    spark.read.schema(Schema.dimApplicantSchema).parquet(factPath)
  }

}
