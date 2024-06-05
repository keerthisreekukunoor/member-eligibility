import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object MemberEligibility {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Member Eligibility")
      .master("local[*]") // Set the master URL to local
      .getOrCreate()

    logger.info("Spark session started")

    val memberEligibilityPath = "data/member_eligibility.csv"
    val memberMonthsPath = "data/member_months.csv"

    try {
      val memberEligibilityDF = loadCSV(spark, memberEligibilityPath)
      val memberMonthsDF = loadCSV(spark, memberMonthsPath)

      logger.info("CSV files loaded successfully")

      val totalMemberMonths = calculateTotalMemberMonths(memberEligibilityDF, memberMonthsDF)
      val memberMonthsPerYear = calculateMemberMonthsPerYear(memberEligibilityDF, memberMonthsDF)

      logger.info("Calculations completed successfully")

      saveAsJson(totalMemberMonths, "output/total_member_months")
      saveAsJson(memberMonthsPerYear, "output/member_months_per_year")

      logger.info("Output saved successfully")
    } catch {
      case e: Exception =>
        logger.error("An error occurred", e)
    } finally {
      spark.stop()
    }

    logger.info("Processing completed")
  }

  def loadCSV(spark: SparkSession, path: String): DataFrame = {
    logger.info(s"Loading CSV file from path: $path")
    spark.read.option("header", "true").csv(path)
  }

  def calculateTotalMemberMonths(memberEligibilityDF: DataFrame, memberMonthsDF: DataFrame): DataFrame = {
    logger.info("Calculating total member months")
    memberEligibilityDF.join(memberMonthsDF, "member_id")
      .groupBy("member_id", "full_name")
      .agg(count("eligibility_date").as("total_member_months"))
  }

  def calculateMemberMonthsPerYear(memberEligibilityDF: DataFrame, memberMonthsDF: DataFrame): DataFrame = {
    logger.info("Calculating member months per year")
    memberEligibilityDF.join(memberMonthsDF, "member_id")
      .withColumn("year", year(col("eligibility_date")))
      .groupBy("member_id", "year")
      .agg(count("eligibility_date").as("total_member_months"))
  }

  def saveAsJson(df: DataFrame, path: String): Unit = {
    logger.info(s"Saving output to path: $path")
    df.write.mode("overwrite").json(path)
  }
}
