import org.apache.spark.sql.SparkSession

object ParquetReaderAndSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetReaderAndSQL").getOrCreate()

    // Read the Parquet file directly into a DataFrame
    val parquetDF = spark.read.parquet("C:\\Users\\SATYAM\\OneDrive\\Desktop\\bigData\\Movies-Analytics-in-Spark-and-Scala\\Spark_SQL\\sparkdatalake\\spark-warehouse\\sparkdatalake.db\\ratings\\part-r-00000-f8e1905c-499f-4600-9744-2e20002f45dc.snappy.parquet")

    // Show the content of the Parquet DataFrame
    parquetDF.show()

    // Perform SQL aggregation on the DataFrame and write the result to a CSV file
    val resultDF = parquetDF
      .groupBy("movieid")
      .agg(avg("rating").cast("decimal(16,2)").alias("Average_ratings"))
      .orderBy("movieid")

    // Write the result DataFrame to a CSV file
    resultDF.repartition(1).write.format("csv").option("header", "true").save("result")


    // Stop SparkSession and exit the application
    spark.stop()
    System.exit(0)
  }
}
