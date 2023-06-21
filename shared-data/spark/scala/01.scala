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
  val diseaseDescriptionsDF: DataFrame = jdbcDFs(0)
  val diseaseSymptomsDF: DataFrame = jdbcDFs(1)

  val diseaseDescriptionsRDD: RDD[Row] = diseaseDescriptionsDF.rdd
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

  val pairDiseaseDescriptionsRDD: RDD[(String, String)] = diseaseDescriptionsRDD
    .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String]))

  val resultRDD =
    pairDiseaseDescriptionsRDD.join(reducedDiseaseSymptomsRDD).map {
      case (key, (description, symptom)) =>
        Row(key, description, symptom)
    }

  val resultSchema = StructType(
    Seq(
      StructField("disease", StringType),
      StructField("description", StringType),
      StructField("symptoms", StringType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  val numberOfRows = resultDF.count().toInt

  resultDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println("\nProblema 1: Obtener todas las enfermedades junto a sus síntomas:")
  doStuffWithManyTables(
    Array("disease_descriptions", "disease_symptoms"),
    problem1
  )

  System.exit(0)
}

main()
