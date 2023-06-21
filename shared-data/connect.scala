import org.apache.spark.sql.{SparkSession, DataFrame}

val DB = sys.env("POSTGRES_DB")
val USER = sys.env("POSTGRES_USER")
val PASSWORD = sys.env("POSTGRES_PASSWORD")
val HOST = sys.env("PG_CONTAINER_NAME")
val PORT = 5432

object Connection {
  def doStuffWithOneTable(
    tableName: String,
    callback: (SparkSession, String, DataFrame) => Unit
  ): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark PostgreSQL connection")
      .getOrCreate()
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$HOST:$PORT/$DB")
      .option("dbtable", tableName)
      .option("user", USER)
      .option("password", PASSWORD)
      .load()

    try {
      callback(spark, tableName, jdbcDF)
    } catch {
      case e: Exception => println(e)
    } finally {
      spark.close()
    }
  }

  def doStuffWithManyTables(
    tableNames: Array[String],
    callback: (SparkSession, Array[String], Array[DataFrame]) => Unit
  ): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark PostgreSQL connection")
      .getOrCreate()
    val jdbcDFs = for (tableName <- tableNames) yield spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$HOST:$PORT/$DB")
      .option("dbtable", tableName)
      .option("user", USER)
      .option("password", PASSWORD)
      .load()

    try {
      callback(spark, tableNames, jdbcDFs)
    } catch {
      case e: Exception => println(e)
    } finally {
      spark.close()
    }
  }
}
