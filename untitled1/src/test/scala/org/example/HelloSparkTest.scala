import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SparkSQLSolutionTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("RevisedSparkSQLSolution logic") {
    import org.apache.spark.implicits._

    val testData = Seq(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      // 这里添加完整的测试数据...
      ("AE686(AE)", "12", "OM691", 2022)
    ).toDF("peer_id", "id_1", "id_2", "year")

    testData.createOrReplaceTempView("peer_data")

    // 这里调用 RevisedSparkSQLSolution 里面的逻辑，但可能需要一些调整
    // 计算每个peer_id的目标年份
    val targetYears = spark.sql(
      """
        SELECT peer_id, year AS target_year
        FROM peer_data
        WHERE INSTR(peer_id, id_2) > 0
      """
    )

    targetYears.show()

    // ... 这里应该 replicates the logic you are testing

    assert(targetYears.count() == N) // N 代表你期望的行的数量

    // 这里添加更多的assert验证逻辑的正确性
  }
}