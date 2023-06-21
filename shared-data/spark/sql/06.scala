import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithManyTables}

def problem3(
  spark: SparkSession,
  tableNames: Array[String],
  jdbcDFs: Array[DataFrame]
) = {
  val diseaseSymptomsTable = tableNames(0)
  val diseaseSymptomsView = s"${diseaseSymptomsTable}_view"
  val diseaseSymptomsDF = jdbcDFs(0)
  diseaseSymptomsDF.createOrReplaceTempView(diseaseSymptomsView)

  val newDiseaseSymptomsView = "newDiseaseSymptomsView"
  val newDiseaseSymptomsDF = diseaseSymptomsDF
    .selectExpr("disease", "stack(17, symptom1, symptom2, symptom3, symptom4, symptom5, symptom6, symptom7, symptom8, symptom9, symptom10, symptom11, symptom12, symptom13, symptom14, symptom15, symptom16, symptom17) AS (symptom)")
    .where("symptom IS NOT NULL")
    .distinct()
    .orderBy("disease")
  newDiseaseSymptomsDF.createOrReplaceTempView(newDiseaseSymptomsView)

  val symptomSeveritiesTable = tableNames(1)
  val symptomSeveritiesView = s"${symptomSeveritiesTable}_view"
  val symptomSeveritiesDF = jdbcDFs(1)
  symptomSeveritiesDF.createOrReplaceTempView(symptomSeveritiesView)

  val resultDF = spark.sql(s"""
    SELECT
      DISTINCT ds.disease
    FROM
      $newDiseaseSymptomsView ds
    INNER JOIN
      $symptomSeveritiesView ss
    ON
      ds.symptom = ss.symptom
    WHERE
      ss.weight <= 1""")
  resultDF.show()
}

def main() = {
  println("Problema 6: Obtén las precauciones relacionadas con los síntomas con una gravedad menor o igual a 1:")
  doStuffWithManyTables(
    Array("disease_symptoms", "symptom_severities"),
    problem3
  )
  System.exit(0)
}

main()
