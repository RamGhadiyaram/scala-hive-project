

import org.apache.spark.sql.SparkSession

object SparkHiveExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .config("spark.sql.warehouse.dir", "./user/hive/warehouse")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    // Create the database
    spark.sql("CREATE DATABASE IF NOT EXISTS sample_db")

    // Use the database
    spark.sql("USE sample_db")

    // Create the table
    spark.sql(
      """
      CREATE TABLE IF NOT EXISTS sample_table (
        id INT,
        name STRING,
        age INT
      )
      """)

    // Insert some data into the table
    spark.sql(
      """
      INSERT INTO sample_table
      VALUES (1, 'Bharanee S', 30), (2, 'Audrey ', 25)
      """)

    // Run a sample SELECT statement
    val result = spark.sql("SELECT * FROM sample_table")

    // Show the result
    result.show()
    spark.sql("SHOW TABLES").show()
    // Create the partitioned table
    spark.sql(
      """
      CREATE TABLE IF NOT EXISTS sample_partitioned_table (
        id INT,
        name STRING
      )
      PARTITIONED BY (age INT)
      """)

    // Insert some data into the table
    spark.sql(
      """
      INSERT INTO sample_partitioned_table
      PARTITION (age = 30)
      VALUES (1, 'Ram Ghadiyaram'), (2, 'Atanu Basak')
      """)

    // Run a sample SELECT statement
    val result1 = spark.sql("SELECT * FROM sample_partitioned_table")

    // Show the result
    result1.show()
    spark.stop()
  }
}
