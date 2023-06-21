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

  val symptomsView = "symptoms"
  val symptomsDF = diseaseSymptomsDF
    .groupBy("disease")
    .agg(
      concat_ws(
        ", ",
        collect_set("symptom1"),
        collect_set("symptom2"),
        collect_set("symptom3"),
        collect_set("symptom4"),
        collect_set("symptom5"),
        collect_set("symptom6"),
        collect_set("symptom7"),
        collect_set("symptom8"),
        collect_set("symptom9"),
        collect_set("symptom10"),
        collect_set("symptom11"),
        collect_set("symptom12"),
        collect_set("symptom13"),
        collect_set("symptom14"),
        collect_set("symptom15"),
        collect_set("symptom16"),
        collect_set("symptom17")
      )
      .alias("symptoms")
    )
  symptomsDF.createOrReplaceTempView(symptomsView)

  val diseaseDescriptionsTable = tableNames(1)
  val diseaseDescriptionsView = s"${diseaseDescriptionsTable}_view"
  val diseaseDescriptionsDF = jdbcDFs(1)
  diseaseDescriptionsDF.createOrReplaceTempView(diseaseDescriptionsView)

  val resultDF = spark.sql(s"""
    SELECT
      DISTINCT s.disease, d.description
    FROM
      $symptomsView s
    INNER JOIN
      $diseaseDescriptionsView d
    ON
      s.disease = d.disease
    WHERE
      s.symptoms ilike '%high_fever%'""")
  resultDF.show()
}

def main() = {
  println("Problema 3: Obtén todas las enfermedades y descripciones de las enfermedades que tienen el síntoma 'Fiebre alta':")
  doStuffWithManyTables(
    Array("disease_symptoms", "disease_descriptions"),
    problem3
  )
  System.exit(0)
}

main()
