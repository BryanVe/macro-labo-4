import org.apache.spark.sql.SparkSession

val DB = sys.env("POSTGRES_DB")
val USER = sys.env("POSTGRES_USER")
val PASSWORD = sys.env("POSTGRES_PASSWORD")
val HOST = sys.env("PG_CONTAINER_NAME")
val PORT = 5432
val TABLE = "symptom_severities"

val spark = SparkSession.builder()
  .appName("Spark PostgreSQL connection")
  .getOrCreate()

val jdbcDF = spark.read
  .format("jdbc")
  .option("url", s"jdbc:postgresql://$HOST:$PORT/$DB")
  .option("dbtable", TABLE)
  .option("user", USER)
  .option("password", PASSWORD)
  .load()

jdbcDF.createOrReplaceTempView("symptom_severities_view")
val resultDF = spark.sql("SELECT * FROM symptom_severities_view WHERE weight == 4")
resultDF.show()
