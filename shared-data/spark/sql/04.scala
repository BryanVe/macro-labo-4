import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithManyTables}

def problem3(
  spark: SparkSession,
  tableNames: Array[String],
  jdbcDFs: Array[DataFrame]
) = {
  val diseaseDescriptionsTable = tableNames(0)
  val diseaseDescriptionsView = s"${diseaseDescriptionsTable}_view"
  val diseaseDescriptionsDF = jdbcDFs(0)
  diseaseDescriptionsDF.createOrReplaceTempView(diseaseDescriptionsView)

  val diseasePrecautions = tableNames(1)
  val diseasePrecautionsView = s"${diseasePrecautions}_view"
  val diseasePrecautionsDF = jdbcDFs(1)
  diseasePrecautionsDF.createOrReplaceTempView(diseasePrecautionsView)

  val resultDF = spark.sql(s"""
    SELECT
      d.description, p.precaution1, p.precaution2, p.precaution3, p.precaution4
    FROM
      $diseaseDescriptionsView d
    INNER JOIN
      $diseasePrecautionsView p
    ON
      d.disease = p.disease
    WHERE
      d.disease='Malaria'""")
  resultDF.show()
}

def main() = {
  println("Problema 4: Obtén la descripción y las precauciones para la enfermedad 'Malaria':")
  doStuffWithManyTables(
    Array("disease_descriptions", "disease_precautions"),
    problem3
  )
  System.exit(0)
}

main()
