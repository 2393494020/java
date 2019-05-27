package com.iss

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable

object FrtApp {
  val user = "fzpuser"
  val password = "Fzp@2016"
  val mysqlDriver = "com.mysql.jdbc.Driver"
  // val mysqlJdbcUrl = s"jdbc:mysql://192.168.200.36:3306/fzpdb?user=${user}&password=${password}&useSSL=false&rewriteBatchedStatements=true"
  val mysqlJdbcUrl = s"jdbc:mysql://132.122.38.47:3306/fzpdb?user=${user}&password=${password}&useSSL=false&rewriteBatchedStatements=true"

  def main(args: Array[String]): Unit = {
    Class.forName(mysqlDriver)
    val mysqlConnect = DriverManager.getConnection(mysqlJdbcUrl)
    val mysqlStmt = mysqlConnect.createStatement()
    mysqlConnect.setAutoCommit(false)

    val sql =
      """
        |SELECT
        |count(*),
        |score00,
        |score01,
        |score02,
        |score03,
        |score04,
        |score05,
        |score06,
        |score07,
        |score08,
        |score09,
        |score10,
        |score11,
        |score12,
        |score13,
        |score14,
        |score15,
        |score16,
        |score17,
        |score18,
        |score19,
        |score20,
        |score21,
        |score22,
        |score23 fROM `frt_x_user_3` WHERE
        |score00 is not null and
        |score01 is not null and
        |score02 is not null and
        |score03 is not null and
        |score04 is not null and
        |score05 is not null and
        |score06 is not null and
        |score07 is not null and
        |score08 is not null and
        |score09 is not null and
        |score10 is not null and
        |score11 is not null and
        |score12 is not null and
        |score13 is not null and
        |score14 is not null and
        |score15 is not null and
        |score16 is not null and
        |score17 is not null and
        |score18 is not null and
        |score19 is not null and
        |score20 is not null and
        |score21 is not null and
        |score22 is not null and
        |score23 is not null
        | GROUP BY
        |score00,
        |score01,
        |score02,
        |score03,
        |score04,
        |score05,
        |score06,
        |score07,
        |score08,
        |score09,
        |score10,
        |score11,
        |score12,
        |score13,
        |score14,
        |score15,
        |score16,
        |score17,
        |score18,
        |score19,
        |score20,
        |score21,
        |score22,
        |score23
        |ORDER BY count(*) desc
      """.stripMargin.split("\\s+").map(_.trim).filter(_.length > 0).mkString(" ")

    val rs = mysqlStmt.executeQuery(sql)
    while (rs.next()) {
      val args = mutable.ListBuffer[Int]()
      2.to(25).foreach(i => {
        args.append(rs.getInt(i))
      })

      val positiveSum = args.filter(_ > 0).sum
      val negativeSum = args.filter(_ < 0).sum

      if (positiveSum < 24) {
        var i = 0
        val j = 1
        0.until(23).foreach(i => {
          1.to(23).foreach(j => {

          })
        })
      }

      println(s"${args.mkString("\t")}, $positiveSum, $negativeSum")
    }

    rs.close()
    mysqlConnect.close()

    //    val context = new SparkContext()
    //    val sqlContext = new SQLContext(context)
    //    val accountMap = Map("url" -> mysqlJdbcUrl)
    //    val df = sqlContext.load("jdbc", accountMap + ("dbtable" -> s"($sql) as frt_alias"))


    //    df.collect().foreach(row => {
    //      println(row.getLong(0))
    //      val args = mutable.ListBuffer[Int]()
    //      1.to(24).foreach(i => {
    //        args.append(row.getInt(i))
    //      })
    //
    //      println(args.mkString("\t"))

    //      val positiveSum = args.filter(_ > 0).sum
    //      val negativeSum = args.filter(_ < 0).sum
    //      val newRow = Row.fromSeq(row.toSeq ++ Seq[Int](positiveSum, negativeSum))
    //
    //      val record = mutable.ListBuffer[Int]()
    //      0.to(26).foreach(i => {
    //        if (i == 0)
    //          record.append(newRow.getLong(i).toInt)
    //        else
    //          record.append(newRow.getInt(i))
    //      })
    //      println(record.mkString("\t"))
    //    })

    /*val addition = collection.mutable.ListBuffer[StructField]()
    addition.appendAll(df.schema.seq)
    addition.append(StructField("positiveSum", IntegerType))
    addition.append(StructField("negativeSum", IntegerType))

    sqlContext.createDataFrame(context.parallelize(rows), StructType(addition))*/


    //    context.stop()
  }
}
