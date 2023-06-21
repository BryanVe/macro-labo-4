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
  val querySymptom = "headache"

  val diseasePrecautionsDF: DataFrame = jdbcDFs(0)
  val diseaseSymptomsDF: DataFrame = jdbcDFs(1)

  val diseasePrecautionsRDD: RDD[Row] = diseasePrecautionsDF.rdd
  val diseaseSymptomsRDD: RDD[Row] = diseaseSymptomsDF.rdd

  val flattenDiseaseSymptomsRDD: RDD[(String, String)] =
    diseaseSymptomsRDD.flatMap(row => {
      val disease = row.getString(0)
      val symptoms =
        row.toSeq.tail
          .filter(_ == querySymptom)
          .distinct
          .map(_.asInstanceOf[String])

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

  val pairDiseasePrecautionsRDD: RDD[(String, String)] = diseasePrecautionsRDD
    .map(row => (row(0).asInstanceOf[String], row.toSeq.tail.mkString(", ")))

  val resultRDD =
    pairDiseasePrecautionsRDD.join(reducedDiseaseSymptomsRDD).map {
      case (key, (precautions, symptom)) =>
        Row(key, precautions, symptom)
    }

  val resultSchema = StructType(
    Seq(
      StructField("disease", StringType),
      StructField("precautions", StringType),
      StructField("symptom", StringType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  val numberOfRows = resultDF.count().toInt

  resultDF.show(numberOfRows, false)
  spark.close()
}

def main() = {
  println(
    "\nProblema 8: Obtén las enfermedades y sus precauciones correspondientes para aquellas enfermedades que tienen el síntoma 'headache'"
  )
  doStuffWithManyTables(
    Array("disease_precautions", "disease_symptoms"),
    problem1
  )

  System.exit(0)
}

main()
