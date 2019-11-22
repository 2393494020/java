package com.gsta

import java.sql.{DriverManager, Statement}

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object GenerateWeekEcoStability extends Util {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main00(args: Array[String]): Unit = {
    1.to(4).foreach(i => {
      println(s"`week1to4_class0${i}_cv_max`    float,")
      println(s"`week1to4_class0${i}_cv_min`    float,")
      println(s"`week1to4_class0${i}_cv_mean`   float,")
      println(s"`week1to4_class0${i}_cv_std`    float,")
      println(s"`week1to4_class0${i}_cv_var`    float,")
      println(s"`week1to4_class0${i}_cv_cv`     float,")
    })
  }

  def main(args: Array[String]): Unit = {
    val dataFile = Source.fromFile("E:\\work\\zhc\\kpi_docs\\dwprb_0603_0804.csv")
    val it = dataFile.getLines()
    val columns = it.next().split(",")
    val begin = columns.indexWhere(_ == "`H2019060300`")
    val end = columns.indexWhere(_ == "`H2019063023`")

    // mysql
    val table = "week_eco_stability_0603_0630"
    Class.forName(mysqlDriver)
    val mysqlConnect = DriverManager.getConnection(mysqlJdbcUrl)
    val mysqlStmt = mysqlConnect.createStatement()
    mysqlConnect.setAutoCommit(false)
    mysqlStmt.execute(s"delete from `$table`")
    mysqlConnect.commit()

    val sqlClause = mutable.ListBuffer[String]()
    val categoryMap = loadCategory(mysqlStmt)
    while (it.hasNext) {
      val values = it.next().trim.split(",")
      val key = (values(1), values(3))
      val targetValues = values.slice(begin, end + 1)
      if (targetValues.count(_ == "null") == 0 && categoryMap.contains(key)) {
        val merge_after_is_pure = categoryMap(key).head
        val categoryList = categoryMap(key).slice(1, 5)
        val periodList = categoryMap(key).slice(5, 9)

        val insertValues = mutable.ListBuffer[String]()
        insertValues.appendAll(values.slice(1, 5).map(x => if (isInteger(x)) x else s"'$x'") ++ List("null", merge_after_is_pure) ++ (categoryList ++ periodList).map(category => s"'$category'"))

        if (merge_after_is_pure == "0") {
          val hourList = periodToList(periodList.head)
          if (hourList.isEmpty) {
            insertValues.appendAll(List.fill(24 * 9)(null))
          } else {
            val w1 = 1.to(7).map(day => targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat))
            val w2 = 8.to(14).map(day => targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat))
            val w3 = 15.to(21).map(day => targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat))
            val w4 = 22.to(28).map(day => targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat))

            val absValues1vs2 = 1.to(7).flatMap(day => hourList.map(hour => math.abs(w1(day - 1)(hour) - w2(day - 1)(hour))))
            val coefficientList1vs2 = coefficient(absValues1vs2)
            insertValues.appendAll(coefficientList1vs2.map(_.toString))
            insertValues.appendAll(List.fill(18)(null))

            val absValues2vs3 = 1.to(7).flatMap(day => hourList.map(hour => math.abs(w2(day - 1)(hour) - w3(day - 1)(hour))))
            val coefficientList2vs3 = coefficient(absValues2vs3)
            insertValues.appendAll(coefficientList2vs3.map(_.toString))
            insertValues.appendAll(List.fill(18)(null))

            val absValues3vs4 = 1.to(7).flatMap(day => hourList.map(hour => math.abs(w3(day - 1)(hour) - w4(day - 1)(hour))))
            val coefficientList3vs4 = coefficient(absValues3vs4)
            insertValues.appendAll(coefficientList3vs4.map(_.toString))
            insertValues.appendAll(List.fill(18)(null))

            coefficientList1vs2.indices.foreach(i => {
              val coefficient1vs2 = coefficientList1vs2(i)
              val coefficient2vs3 = coefficientList2vs3(i)
              val coefficient3vs4 = coefficientList3vs4(i)
              insertValues.appendAll(coefficient(List(coefficient1vs2, coefficient2vs3, coefficient3vs4)).map(_.toString))
              insertValues.appendAll(List.fill(18)(null))
            })
          }
        } else {
          val coefficientMap = mutable.LinkedHashMap[String, mutable.ListBuffer[Seq[Float]]]()
          val weekPairList = List((1, 8), (8, 15), (15, 22))
          weekPairList.indices.foreach(pair_i => {
            val weekPair = weekPairList(pair_i)
            categoryList.indices.foreach(i => {
              val category = categoryList(i)
              val period = periodList(i)
              if (category == null || period == null || period.isEmpty) {
                insertValues.appendAll(List.fill(6)(null))
              } else {
                val subCategoryList = category.map(_.toString.toInt)
                val hourList = periodToList(period)
                val w1_start = weekPair._1
                val w2_start = weekPair._2
                val w1 = w1_start.to(w1_start + 6).map(day => {
                  if (subCategoryList.contains(day - pair_i * 7))
                    targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat)
                  else
                    Array.empty[Float]
                })
                val w2 = w2_start.to(w2_start + 6).map(day => {
                  if (subCategoryList.contains(day - (pair_i + 1) * 7))
                    targetValues.slice((day - 1) * 24, (day - 1) * 24 + 24).map(_.toFloat)
                  else
                    Array.empty[Float]
                })
                /*logger.debug(s"${subCategoryList.mkString(",")}")
                logger.debug(s"${hourList.mkString(",")}")
                logger.debug(s"$w1_start")
                logger.debug(s"$w2_start")
                w1.foreach(values => logger.debug(values.mkString(",")))
                w2.foreach(values => logger.debug(values.mkString(",")))*/
                val absValues = subCategoryList.flatMap(day => {
                  hourList.map(hour => {
                    math.abs(w1(day - 1)(hour) - w2(day - 1)(hour))
                  })
                })
                val coefficientList = coefficient(absValues)
                insertValues.appendAll(coefficientList.map(_.toString))
                if (!coefficientMap.contains(category))
                  coefficientMap(category) = mutable.ListBuffer[Seq[Float]]()
                coefficientMap(category).append(coefficientList)
              }
            })
          })

          0.to(5).foreach(coefficient_i => {
            categoryList.foreach(category => {
              if (!coefficientMap.contains(category)) {
                insertValues.appendAll(List.fill(6)(null))
              } else {
                val coefficientSeq = coefficientMap(category)
                val coefficient1vs2 = coefficientSeq.head(coefficient_i)
                val coefficient2vs3 = coefficientSeq(1)(coefficient_i)
                val coefficient3vs4 = coefficientSeq.last(coefficient_i)
                insertValues.appendAll(coefficient(List(coefficient1vs2, coefficient2vs3, coefficient3vs4)).map(_.toString))
              }
            })
          })
        }
        sqlClause.append(s"(${insertValues.mkString(",").replace("'null'", "null")})")
      }
    }
    dataFile.close()

    val pageSize = 1000
    val pageCount = sqlClause.length / pageSize
    0.to(pageCount).foreach(page => {
      val sql = s"insert into `$table` values ${sqlClause.slice(page * pageSize, (page + 1) * pageSize).mkString(",")}"
      mysqlStmt.execute(sql)
      mysqlConnect.commit()
    })

    mysqlStmt.close()
    mysqlConnect.close()
  }

  def coefficient(values: Seq[Float]): Seq[Float] = {
    val sum = values.sum
    val max = values.max
    val min = values.min
    val mean = sum / values.length
    val variance = values.map(x => math.pow(x - mean, 2)).sum / values.length
    val std = math.sqrt(variance)
    List(max, min, mean, std.toFloat, variance.toFloat, if (mean == 0) 0 else std.toFloat / mean)
  }

  def loadCategory(mysqlStmt: Statement): Map[(String, String), Seq[String]] = {
    val values = mutable.HashMap[(String, String), Seq[String]]()
    val rs = mysqlStmt.executeQuery(s"SELECT `base_statn_id`, `cell_id`, `merge_after_is_pure`, `category_01`, `category_02`, `category_03`, `category_04`, `c01_dwprb_1st_p`, `c02_dwprb_1st_p`, `c03_dwprb_1st_p`, `c04_dwprb_1st_p` FROM `frt_cell_type` where `merge_after_is_pure` is not null")
    while (rs.next()) {
      values((rs.getInt(1).toString, rs.getInt(2).toString)) = List(rs.getInt(3).toString) ++ 4.to(11).map(i => rs.getString(i))
    }
    rs.close()
    values.toMap
  }
}
