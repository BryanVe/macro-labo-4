import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext._

import Connection.{doStuffWithManyTables}

def problem1(
    spark: SparkSession,
    tableNames: Array[String],
    jdbcDFs: Array[DataFrame]
) = {
  val querySymptom = "high_fever"

  val diseaseDescriptionsDF: DataFrame = jdbcDFs(0)
  val diseaseSymptomsDF: DataFrame = jdbcDFs(1)

  val diseaseDescriptionsRDD: RDD[Row] = diseaseDescriptionsDF.rdd
  val diseaseSymptomsRDD: RDD[Row] = diseaseSymptomsDF.rdd

  val flattenDiseaseSymptomsRDD: RDD[(String, String)] =
    diseaseSymptomsRDD.flatMap(row => {
      val disease = row.getString(0)
      val symptoms =
        row.toSeq.tail.filter(_ == querySymptom).distinct.map(_.asInstanceOf[String])

      symptoms.map(symptom => (disease, symptom)).iterator
    })

  val reducedDiseaseSymptomsRDD = flattenDiseaseSymptomsRDD
    .reduceByKey((a, b) => a + ", " + b)
    .mapValues(
      _.split(", ")
        .map(_.trim)
        .distinct
        .mkString(", ")
    )

  val resultsDF = reducedDiseaseSymptomsRDD.toDF("disease", "symptoms")
  val resultsWithDescriptionsDF = diseaseDescriptionsDF
    .as("dd")
    .join(resultsDF.as("r"), $"dd.disease" === $"r.disease")
    .select($"dd.*")
  val numberOfRows = resultsWithDescriptionsDF.count().toInt

  resultsWithDescriptionsDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println("\nProblema 3: Obtén todas las enfermedades y descripciones de las enfermedades que tienen el síntoma 'Fiebre alta'")
  doStuffWithManyTables(
    Array("disease_descriptions", "disease_symptoms"),
    problem1
  )

  System.exit(0)
}

main()
