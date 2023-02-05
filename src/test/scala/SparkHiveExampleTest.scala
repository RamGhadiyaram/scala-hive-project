

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
class SparkHiveExampleTest extends AnyFunSuite  {


  test("SparkHiveExample") {
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExampleTest")
      .config("spark.sql.warehouse.dir", "./user/hive/warehouse")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    SparkHiveExample.main(Array.empty)

    val result = spark.sql("SELECT * FROM sample_table")
    assert(result.count() == 2)
    assert(result.columns.length == 3)

    val result1 = spark.sql("SELECT * FROM sample_partitioned_table")
    assert(result1.count() == 2)
    assert(result1.columns.length == 3)

  }
}

