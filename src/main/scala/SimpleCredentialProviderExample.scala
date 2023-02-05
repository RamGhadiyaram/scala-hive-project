import org.apache.spark.sql.SparkSession

object SimpleCredentialProviderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveMetastoreExample")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.sql.catalogImplementation", "hive")
    spark.conf.set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore-host:9083")

    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val df = spark.sql("SELECT * FROM <database>.<table>")
    df.show()

    spark.stop()
  }
}