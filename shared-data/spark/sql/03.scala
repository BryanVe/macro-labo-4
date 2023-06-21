import org.apache.spark.sql.{SparkSession, DataFrame}
import Connection.{doStuffWithManyTables}

def problem1(
  spark: SparkSession,
  tableNames: Array[String],
  jdbcDFs: Array[DataFrame]
) = {
  val diseaseSymptomsTable = tableNames(0)
  val diseaseSymptomsView = s"${diseaseSymptomsTable}_view"
  val diseaseSymptomsDF = jdbcDFs(0)
  diseaseSymptomsDF.createOrReplaceTempView(diseaseSymptomsView)

  val symptomView = "symptoms"
  val symptomsDF = spark.sql(s"""
    SELECT
      disease,
      CONCAT(
        symptom1,
        ';',
        coalesce(symptom2, '-'),
        ';',
        coalesce(symptom3, '-'),
        ';',
        coalesce(symptom4, '-'),
        ';',
        coalesce(symptom5, '-'),
        ';',
        coalesce(symptom6, '-'),
        ';',
        coalesce(symptom7, '-'),
        ';',
        coalesce(symptom8, '-'),
        ';',
        coalesce(symptom9, '-'),
        ';',
        coalesce(symptom10, '-'),
        ';',
        coalesce(symptom11, '-'),
        ';',
        coalesce(symptom12, '-'),
        ';',
        coalesce(symptom13, '-'),
        ';',
        coalesce(symptom14, '-'),
        ';',
        coalesce(symptom15, '-'),
        ';',
        coalesce(symptom16, '-'),
        ';',
        coalesce(symptom17, '-')
      ) as symptoms
    FROM
      $diseaseSymptomsView""")
  symptomsDF.createOrReplaceTempView(symptomView)

  val diseaseDescriptionsTable = tableNames(1)
  val diseaseDescriptionsView = s"${diseaseDescriptionsTable}_view"
  val diseaseDescriptionsDF = jdbcDFs(1)
  diseaseDescriptionsDF.createOrReplaceTempView(diseaseDescriptionsView)

  val resultDF = spark.sql(s"""
    SELECT
      DISTINCT s.disease, d.description
    FROM
      $symptomView s
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
    problem1
  )
  System.exit(0)
}

main()
