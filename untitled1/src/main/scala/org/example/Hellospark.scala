package org.example

import org.apache.spark.sql.SparkSession

object RevisedSparkSQLSolution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Revised Spark SQL Solution")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      ("ABC17969(AB)", "2", "CDC52533", 2022),
      ("ABC17969(AB)", "3", "DEC59161", 2023),
      ("ABC17969(AB)", "4", "F43874", 2022),
      ("ABC17969(AB)", "5", "MY06154", 2021),
      ("ABC17969(AB)", "6", "MY4387", 2022),
      ("AE686(AE)", "7", "AE686", 2023),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2021),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OM691", 2022)
    ).toDF("peer_id", "id_1", "id_2", "year")

    data.createOrReplaceTempView("peer_data")

    // 计算每个peer_id的目标年份
    val targetYears = spark.sql(
      """
        SELECT peer_id, year AS target_year
        FROM peer_data
        WHERE INSTR(peer_id, id_2) > 0
      """
    )

    targetYears.show()

    targetYears.createOrReplaceTempView("target_years")

    // 计算每个年份的计数，考虑目标年份
    val preCount = spark.sql(
      """
        SELECT pd.peer_id, pd.year as peer_year, ty.target_year as target_year
        FROM peer_data pd
        JOIN target_years ty ON pd.peer_id = ty.peer_id
        WHERE pd.year <= ty.target_year
      """
    )

    preCount.show()

    preCount.createOrReplaceTempView("preCount")

    val count = spark.sql(
      """
        SELECT peer_id, peer_year, count(*) as cnt
        FROM preCount
        GROUP BY peer_id,peer_year
        ORDER BY peer_id, peer_year desc
      """
    )

    count.show()

    count.createOrReplaceTempView("count")

//    val results = spark.sql("""
//
//  SELECT
//    peer_id,
//    peer_year,
//    SUM(cnt) OVER(PARTITION BY peer_id ORDER BY peer_year DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt
//  FROM count
//""")
//
//    results.show()

    val results = spark.sql("""
      WITH cumulative_counts AS (
  SELECT
    peer_id,
    peer_year,
    SUM(cnt) OVER(PARTITION BY peer_id ORDER BY peer_year DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cnt
  FROM count
)
SELECT peer_id, peer_year
FROM cumulative_counts
WHERE cumulative_cnt < 5
""")

    results.show()

  }
}