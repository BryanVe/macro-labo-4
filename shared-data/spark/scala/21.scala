import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}
import org.apache.spark.SparkContext._

import Connection.{doStuffWithManyTables}

def problem1(
    spark: SparkSession,
    tableNames: Array[String],
    jdbcDFs: Array[DataFrame]
) = {
  val queryDisease = "Malaria"

  val symptomSeveritiesDF: DataFrame = jdbcDFs(0)
  val diseaseSymptomsDF: DataFrame = jdbcDFs(1)

  val symptomSeveritiesRDD: RDD[Row] = symptomSeveritiesDF.rdd
  val diseaseSymptomsRDD: RDD[Row] = diseaseSymptomsDF.rdd

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

  val filteredReducedDiseaseSymptomsRDD = reducedDiseaseSymptomsRDD
    .filter(row => row._1 == queryDisease)

  val querySymptoms = filteredReducedDiseaseSymptomsRDD.flatMap(row => {
    val symptoms = row._2

    symptoms.split(", ").map(_.trim).map(row => (row, row)).iterator
  })

  val pairSymptomSeveritiesRDD: RDD[(String, Int)] = symptomSeveritiesRDD
    .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Int]))

  val resultRDD =
    querySymptoms.join(pairSymptomSeveritiesRDD).map {
      case (symptom, (s, severity)) =>
        Row(symptom, severity)
    }

  val resultSchema = StructType(
    Seq(
      StructField("symptom", StringType),
      StructField("severity", IntegerType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  val numberOfRows = resultDF.count().toInt

  println(s"Enfermedad: $queryDisease")
  resultDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println(
    "\nProblema 21: Obtener los síntomas con su respectiva severidad para una enfermedad específica"
  )
  doStuffWithManyTables(
    Array("symptom_severities", "disease_symptoms"),
    problem1
  )

  System.exit(0)
}

main()
