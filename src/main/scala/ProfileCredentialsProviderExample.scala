import org.apache.spark.sql.SparkSession
class ProfileCredentialsProviderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveMetastoreExample")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.sql.catalogImplementation", "hive")
    spark.conf.set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore-host:9083")

    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.access.key", "<your-access-key-id>")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.secret.key", "<your-secret-access-key>")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.region", "<your-region>")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.aws.credentials.provider", "file://~/.aws/credentials")

    val df = spark.sql("SELECT * FROM <database>.<table>")
    df.show()

    spark.stop()
  }

}
