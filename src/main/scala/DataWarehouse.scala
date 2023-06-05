import com.typesafe.config.{Config, ConfigFactory}
import common.{DataLakeRead, Utility}
import control.{ProcessInvalidData, WriteControlTable}
import dimensions.{DimApplicant, DimCompany, DimJobInfo, DimJobPostStatus}
import facts.{FactJobApplicant, FactJobPost}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataWarehouse {

  /**
   * Main function. Entry point for the application that reads from the datalake and load the data in a datawarehouse
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()
    val spark: SparkSession = Utility.getSparkSession(config)

    // extract data from datalake (raw data)
    val (validData: DataFrame, invalidData: DataFrame) = DataLakeRead(config, spark)

    // handle corrupted records
    if(invalidData.take(1).size > 0)
      ProcessInvalidData(invalidData, config)

    if(validData.take(1).size > 0) {
      // load dimensions
      DimCompany.loadDimension(validData, config, spark)
      DimJobInfo.loadDimension(validData, config, spark)
      DimJobPostStatus.loadDimension(validData, config, spark)
      DimApplicant.loadDimension(validData, config, spark)

      // load fact tables
      FactJobPost.loadFact(validData, config, spark)
      FactJobApplicant.loadFact(validData, config, spark)

      // write control table
      WriteControlTable(validData, config)
    }
  }

}
