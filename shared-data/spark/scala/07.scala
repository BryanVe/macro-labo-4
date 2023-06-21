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
  val keyword = "respiratory"

  val diseaseSymptomsDF: DataFrame = jdbcDFs(0)
  val diseaseDescriptionsDF: DataFrame = jdbcDFs(1)

  val diseaseSymptomsRDD: RDD[Row] = diseaseSymptomsDF.rdd
  val diseaseDescriptionsRDD: RDD[Row] = diseaseDescriptionsDF.rdd

  val filteredDiseaseDescriptionsRDD: RDD[(String, String)] =
    diseaseDescriptionsRDD
      .filter(row => row(1).asInstanceOf[String].contains(keyword))
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))

  val flattenDiseaseSymptomsRDD: RDD[(String, String)] =
    diseaseSymptomsRDD.flatMap(row => {
      val disease = row.getString(0)
      val symptoms =
        row.toSeq.tail.filter(_ != null).distinct.map(_.asInstanceOf[String])

      symptoms.map(symptom => (disease, symptom)).iterator
    })

  val reducedDiseaseSymptomsRDD: RDD[(String, String)] =
    flattenDiseaseSymptomsRDD
      .reduceByKey((a, b) => a + ", " + b)
      .mapValues(
        _.split(", ")
          .map(_.trim)
          .distinct
          .mkString(", ")
      )
      .map(row => (row._1.asInstanceOf[String], row._2.asInstanceOf[String]))

  val resultRDD =
    filteredDiseaseDescriptionsRDD.join(reducedDiseaseSymptomsRDD).map {
      case (disease, (description, symptoms)) =>
        Row(disease, symptoms, description)
    }

  val resultSchema = StructType(
    Seq(
      StructField("disease", StringType),
      StructField("symptoms", StringType),
      StructField("description", StringType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  val numberOfRows = resultDF.count().toInt

  resultDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println(
    "\nProblema 7: Obtén las enfermedades y sus síntomas correspondientes para aquellas enfermedades que tienen una descripción que contiene la palabra 'respiratory'"
  )
  doStuffWithManyTables(
    Array("disease_symptoms", "disease_descriptions"),
    problem1
  )

  System.exit(0)
}

main()
