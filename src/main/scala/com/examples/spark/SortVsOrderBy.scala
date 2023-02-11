package com.examples.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, spark_partition_id}

import scala.io.Source

object SortVsOrderBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("spark://localhost:7077")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.memory.offHeap.enabled", true)
      .config("spark.memory.offHeap.size", "1g")
      .appName("SortVsOrderBy")
      .getOrCreate()

    import spark.implicits._

    val employees =
      Seq(
        ("Mark", "Sales", "AL", 80000, 24, 90000),
        ("John", "Sales", "AL", 76000, 46, 10000),
        ("Alex", "Sales", "CA", 71000, 20, 13000),
        ("Chris", "Finance", "CT", 80000, 42, 13000),
        ("Oliver", "Finance", "DE", 89000, 30, 14000),
        ("Emma", "Finance", "DL", 73000, 46, 29000),
        ("Olivia", "Finance", "IL", 88000, 63, 25000),
        ("Mia", "Marketing", "IN", 90000, 35, 28000),
        ("Henry", "Marketing", "RI", 82000, 40, 11000),
        ("Sam", "Marketing", "RI", 81000, 40, 12000),
        ("Bob", "Finance", "IL", 120000, 40, 91000),
        ("Peter", "Finance", "IL", 120001, 51, 95000),
      ).toDF("employee_name", "department", "state", "salary", "age", "bonus")

    /***
      * SORT BY section
      */
    employees
      .repartition(4)
      .sort(col("salary"))
      .show(truncate = false)

    employees.createOrReplaceTempView("employees")

    spark
      .sql("SELECT /*+ REPARTITION(4) */ employee_name," +
        " department, state, salary, age, bonus FROM employees  SORT BY salary")
      .show(truncate = false)

    /***
      * ORDER BY section
      */
    employees
      .repartition(4)
      .orderBy(col("salary"))
      .show(truncate = false)

    spark
      .sql("SELECT /*+ REPARTITION(4) */ employee_name," +
        " department, state, salary, age, bonus FROM employees  ORDER BY salary")
      .show(truncate = false)

  }
}
