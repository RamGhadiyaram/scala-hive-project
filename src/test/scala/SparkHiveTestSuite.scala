import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

class SparkHiveTestSuite extends AnyFunSuite   with BeforeAndAfterAll {
    var spark: SparkSession = _

    override def beforeAll() {
      // Initialize SparkSession
        spark = SparkSession
        .builder()
        .appName(this.getClass.getName)
        .config("spark.sql.warehouse.dir", "./user/hive/warehouse")
        .config("spark.master", "local[*]")
        .enableHiveSupport()
        .getOrCreate()
      spark.sql("CREATE DATABASE IF NOT EXISTS sample_db")

      // Use the database
      spark.sql("USE sample_db")
      // Initialize Hive tables and load data
      spark.sql("CREATE TABLE test_data (col1 INT, col2 STRING)")
      spark.sql("INSERT INTO test_data VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')")
    }

    test("Test data is loaded correctly") {
      val result = spark.sql("SELECT * FROM test_data").collect()
      assert(result.length == 3)
      assert(result(0).getInt(0) == 1)
      assert(result(0).getString(1) == "test1")
    }

    override def afterAll() {
      // Clean up SparkSession
      spark.stop()
    }

}
