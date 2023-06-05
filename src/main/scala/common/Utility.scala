package common

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object Utility {
  /**
   * function to create and return the spark session
   * @param config configuration file
   * @return the spark session
   */
  def getSparkSession(config: Config): SparkSession = {
    val env: String = config.getString("env")
    if(env == "cloud") {
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", config.getString("awsAccessKey"))
        .config("spark.hadoop.fs.s3a.secret.key", config.getString("awsSecretKey"))
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint","s3.amazonaws.com")
        //.config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .appName("Loka DataLake")
        .getOrCreate()
    }else{
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Loka DataLake")
        .getOrCreate()
    }
  }


  /**
   * extract the datalake s3 base path
   * @param config: configuration file
   * @return: string containing the data lake s3 base path (raw data)
   */
  def getDataLakePath(config: Config): String = {
    val env: String = config.getString("env")
    val path: String = if(env=="cloud") config.getString("cloudDataLakePath") else config.getString("localDataLakePath")
    path
  }

  /**
   *
   * @param config: configuration file
   * @return: string containing control path (table used to perform incremental reads)
   */
  def getControlPath(config: Config): String = {
    val env: String = config.getString("env")
    val path: String = if(env=="cloud") config.getString("cloudControlPath") else config.getString("localControlPath")
    path
  }


  /**
   * Function that return the path for the table that contains the corrupted records extracted from data lake
   * @param config: configuration file
   * @return: string containing the base path for the garbage s3 table
   */
  def getGarbagePath(config: Config): String = {
    val env: String = config.getString("env")
    val path: String = if(env=="cloud") config.getString("cloudGarbagePath") else config.getString("localGarbagePath")
    path
  }


  /**
   * Function that returns the data warehouse s3 location path
   * @param config: configuration file
   * @return: string containing the base path for the data warehouse in s3 bucket
   */
  def getDataWarehousePath(config: Config): String = {
    val env: String = config.getString("env")
    val path: String = if(env=="cloud") config.getString("cloudDataWarehousePath") else config.getString("localDataWarehousePath")
    path
  }

}
