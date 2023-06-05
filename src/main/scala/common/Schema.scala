package common

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

/**
 * Object that contains the schema definitions of the dimensions
 */
object Schema {

  val dimCompanySchema: StructType = new StructType()
    .add("id_dim_company", StringType, false)
    .add("company", StringType, false)
    .add("sector", StringType, false)
    .add("city", StringType, false)

  val dimJobInfoSchema: StructType = new StructType()
    .add("id_dim_job_info", StringType, false)
    .add("job_title", StringType, false)
    .add("benefits", ArrayType(StringType), false)

  val dimJobPostStatusSchema: StructType = new StructType()
    .add("id_dim_job_post_status", StringType, false)
    .add("post_status", StringType, false)

  val dimApplicantSchema: StructType = new StructType()
    .add("id_dim_applicant", StringType, false)
    .add("first_name", StringType, false)
    .add("last_name", StringType, false)
    .add("full_name", StringType, false)
    .add("age", LongType, false)
    .add("skills", ArrayType(StringType), false)

}
