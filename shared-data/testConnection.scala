import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithOneTable, doStuffWithManyTables}

def cb(spark: SparkSession, tableName: String, jdbcDF: DataFrame) = {
  jdbcDF.createOrReplaceTempView(s"${tableName}_view")
  val resultDF = spark.sql(s"SELECT * FROM ${tableName}_view WHERE weight = 4")
  resultDF.show()
}

def cb2(spark: SparkSession, tableNames: Array[String], jdbcDFs: Array[DataFrame]) = {
  jdbcDFs(0).createOrReplaceTempView(s"${tableNames(0)}_view")
  val resultDF = spark.sql(s"SELECT * FROM ${tableNames(0)}_view WHERE weight = 4")
  resultDF.show()
  
  jdbcDFs(1).createOrReplaceTempView(s"${tableNames(1)}_view")
  val resultDF2 = spark.sql(s"SELECT * FROM ${tableNames(1)}_view WHERE disease = 'Malaria'")
  resultDF2.show()
}

def main() = {
  // doStuffWithOneTable("symptom_severities", cb)
  doStuffWithManyTables(Array("symptom_severities", "disease_precautions"), cb2)
  System.exit(0)
}

main()