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
    .where("symptom = 'headache'")
    .distinct()
    .orderBy("disease")
  newDiseaseSymptomsDF.createOrReplaceTempView(newDiseaseSymptomsView)

  val diseasePrecautionsTable = tableNames(1)
  val diseasePrecautionsView = s"${diseasePrecautionsTable}_view"
  val diseasePrecautionsDF = jdbcDFs(1)
  diseasePrecautionsDF.createOrReplaceTempView(diseasePrecautionsView)

  val resultDF = spark.sql(s"""
    SELECT
      dp.*
    FROM
      $newDiseaseSymptomsView ds
    INNER JOIN
      $diseasePrecautionsView dp
    ON
      ds.disease = dp.disease""")
  resultDF.show()
}

def main() = {
  println("Problema 8: Obtén las enfermedades y sus precauciones correspondientes para aquellas enfermedades que tienen el síntoma 'headache':")
  doStuffWithManyTables(
    Array("disease_symptoms", "disease_precautions"),
    problem3
  )
  System.exit(0)
}

main()
