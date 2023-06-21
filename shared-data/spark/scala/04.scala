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
  val queryDisease = "Malaria"

  val diseaseDescriptionsDF: DataFrame = jdbcDFs(0)
  val diseasePrecautionsDF: DataFrame = jdbcDFs(1)

  val diseaseDescriptionsRDD: RDD[Row] = diseaseDescriptionsDF.rdd
  val diseasePrecautionsRDD: RDD[Row] = diseasePrecautionsDF.rdd

  val filteredDiseaseDescriptionsRDD = diseaseDescriptionsRDD
    .filter(row => row(0) == queryDisease)
    .map(row => (row(0), row(1)))
  val filteredDiseasePrecautionsRDD = diseasePrecautionsRDD
    .filter(row => row(0) == queryDisease)
    .map(row => (row(0), row.toSeq.tail.mkString(", ")))

  val resultRDD =
    filteredDiseaseDescriptionsRDD.join(filteredDiseasePrecautionsRDD).map {
      case (key, (value1, value2)) =>
        Row(key, value1, value2)
    }

  val resultSchema = StructType(
    Seq(
      StructField("disease", StringType),
      StructField("description", StringType),
      StructField("precautions", StringType)
    )
  )

  val resultDF = spark.createDataFrame(resultRDD, resultSchema)
  resultDF.show(false)
  spark.close()
}

def main() = {
  println(
    "\nProblema 4: Obtén la descripción y las precauciones para la enfermedad 'Malaria'"
  )
  doStuffWithManyTables(
    Array("disease_descriptions", "disease_precautions"),
    problem1
  )

  System.exit(0)
}

main()
