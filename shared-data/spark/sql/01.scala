import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithManyTables}

def problem1(
  spark: SparkSession,
  tableNames: Array[String],
  jdbcDFs: Array[DataFrame]
) = {
  val diseaseDescriptionTable = tableNames(0)
  val diseaseDescriptionView = s"${diseaseDescriptionTable}_view"
  val diseaseDescriptionsDF = jdbcDFs(0)
  diseaseDescriptionsDF.createOrReplaceTempView(diseaseDescriptionView)

  val diseaseSymptomsTable = tableNames(1)
  val diseaseSymptomsView = s"${diseaseSymptomsTable}_view"
  val diseaseSymptomsDF = jdbcDFs(1)
  diseaseSymptomsDF.createOrReplaceTempView(diseaseSymptomsView)

  val resultDF = spark.sql(s"""
    SELECT
      d.*, s.*
    FROM
      $diseaseDescriptionView d
    INNER JOIN
      $diseaseSymptomsView s
    ON
      d.disease = s.disease""")
  resultDF.show()
}

def main() = {
  println("Problema 1: Obtener todas las enfermedades junto a sus síntomas:")
  doStuffWithManyTables(
    Array("disease_descriptions", "disease_symptoms"),
    problem1
  )
  System.exit(0)
}

main()
