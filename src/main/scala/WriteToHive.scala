import org.apache.spark.sql.SparkSession

import java.io.File

object WriteToHive extends App   {
    val f = new File("./user/hive/warehouse").getAbsolutePath
    println(f)
    val spark = SparkSession
      .builder()
      .appName("SparkHiveExample")
      .config("spark.sql.warehouse.dir", f )
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark.sql("CREATE DATABASE IF NOT EXISTS sample_db")

    // Use the database
    spark.sql("USE sample_db")
    val df = spark.read
      .option("header", "true")
      .csv(new File("src/main/resources/sample_data.csv").getAbsolutePath)


    df.show()

    df.write.mode("overwrite").saveAsTable("sample_db.sample_data")

    spark.stop()
}
