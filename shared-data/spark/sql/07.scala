import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithManyTables}

def problem7(
  spark: SparkSession,
  tableNames: Array[String],
  jdbcDFs: Array[DataFrame]
) = {
  val diseaseSymptomsTable = tableNames(0)
  val diseaseSymptomsView = s"${diseaseSymptomsTable}_view"
  val diseaseSymptomsDF = jdbcDFs(0)
  diseaseSymptomsDF.createOrReplaceTempView(diseaseSymptomsView)

  val diseaseDescriptionsTable = tableNames(1)
  val diseaseDescriptionsView = s"${diseaseDescriptionsTable}_view"
  val diseaseDescriptionsDF = jdbcDFs(1)
  diseaseDescriptionsDF.createOrReplaceTempView(diseaseDescriptionsView)

  val resultDF = spark.sql(s"""
    SELECT
      ds.*
    FROM
      $diseaseSymptomsView ds
    INNER JOIN
      $diseaseDescriptionsView dd
    ON
      dd.description ilike '%respiratory%'"""
  )
    .selectExpr("disease", "stack(17, symptom1, symptom2, symptom3, symptom4, symptom5, symptom6, symptom7, symptom8, symptom9, symptom10, symptom11, symptom12, symptom13, symptom14, symptom15, symptom16, symptom17) AS (symptom)")
    .where("symptom IS NOT NULL")
    .distinct()
    .groupBy("disease")
    .agg(
      concat_ws(", ", collect_list("symptom")).as("symptoms")
    )
    .select("disease", "symptoms")    .orderBy("disease")
  resultDF.show(false)
}

def main() = {
  println("Problema 7: Obtén las enfermedades y sus síntomas correspondientes para aquellas enfermedades que tienen una descripción que contiene la palabra 'respiratory':")
  doStuffWithManyTables(
    Array("disease_symptoms", "disease_descriptions"),
    problem7
  )
  System.exit(0)
}

main()
