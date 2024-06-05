
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MemberEligibilityTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("Member Eligibility Test")
    .master("local[*]")
    .getOrCreate()

  test("calculateTotalMemberMonths") {
    import spark.implicits._

    val memberEligibilityDF = Seq(
      ("1", "John Doe"),
      ("2", "Jane Doe")
    ).toDF("member_id", "full_name")

    val memberMonthsDF = Seq(
      ("1", "2021-01-01"),
      ("1", "2021-02-01"),
      ("2", "2021-01-01")
    ).toDF("member_id", "eligibility_date")

    val result = MemberEligibility.calculateTotalMemberMonths(memberEligibilityDF, memberMonthsDF)
    val expected = Seq(
      ("1", "John Doe", 2),
      ("2", "Jane Doe", 1)
    ).toDF("member_id", "full_name", "total_member_months")

    assert(result.collect() === expected.collect())
  }

  test("calculateMemberMonthsPerYear") {
    import spark.implicits._

    val memberEligibilityDF = Seq(
      ("1", "John Doe"),
      ("2", "Jane Doe")
    ).toDF("member_id", "full_name")

    val memberMonthsDF = Seq(
      ("1", "2021-01-01"),
      ("1", "2021-02-01"),
      ("2", "2021-01-01")
    ).toDF("member_id", "eligibility_date")

    val result = MemberEligibility.calculateMemberMonthsPerYear(memberEligibilityDF, memberMonthsDF)
    val expected = Seq(
      ("1", 2021, 2),
      ("2", 2021, 1)
    ).toDF("member_id", "year", "total_member_months")

    assert(result.collect() === expected.collect())
  }
}
