import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.SparkContext._

import Connection.{doStuffWithManyTables}

def problem1(
    spark: SparkSession,
    tableNames: Array[String],
    jdbcDFs: Array[DataFrame]
) = {
  val querySeverity = 1

  val symptomSeveritiesDF: DataFrame = jdbcDFs(0)
  val diseaseSymptomsDF: DataFrame = jdbcDFs(1)
  val diseasePrecautionsDF: DataFrame = jdbcDFs(2)

  val symptomSeveritiesRDD: RDD[Row] = symptomSeveritiesDF.rdd
  val diseaseSymptomsRDD: RDD[Row] = diseaseSymptomsDF.rdd
  val diseasePrecautionsRDD: RDD[Row] = diseasePrecautionsDF.rdd

  val filteredSymptomSeveritiesRDD = symptomSeveritiesRDD
    .filter(row => row(1).asInstanceOf[Int] <= querySeverity)
    .map(row => row(0))
    .collect
    .toList
    .mkString(", ")

  val flattenDiseaseSymptomsRDD: RDD[(String, String)] =
    diseaseSymptomsRDD.flatMap(row => {
      val disease = row.getString(0)
      val symptoms =
        row.toSeq.tail.filter(_ != null).distinct.map(_.asInstanceOf[String])

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
    .map(row => (row._1, row._2, filteredSymptomSeveritiesRDD))

  val filteredReducedDiseaseSymptomsRDD = reducedDiseaseSymptomsRDD
    .filter(row => {
      val symptoms = row._3.split(", ")

      symptoms.exists(s => row._2.contains(s))
    })
    .map(row => (row._1, row._2))

  val pairDiseasePrecautionsRDD: RDD[(String, String)] = diseasePrecautionsRDD
    .map(row => (row(0).asInstanceOf[String], row.toSeq.tail.mkString(", ")))

  val resultRDD =
    filteredReducedDiseaseSymptomsRDD.join(pairDiseasePrecautionsRDD).map {
      case (disease, (symptoms, precautions)) =>
        Row(disease, precautions, symptoms)
    }

  val resultSchema = StructType(
    Seq(
      StructField("disease", StringType),
      StructField("precautions", StringType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  val numberOfRows = resultDF.count().toInt

  resultDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println(
    "\nProblema 6: Obtén las precauciones relacionadas con los síntomas con una gravedad menor o igual a 1"
  )
  doStuffWithManyTables(
    Array("symptom_severities", "disease_symptoms", "disease_precautions"),
    problem1
  )

  System.exit(0)
}

main()
