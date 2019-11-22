package com.gsta

import java.sql.DriverManager
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object GenerateYear extends Util with Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val field = args(1)
    val table = args(2)
    val schemaFile = Source.fromFile(args(3))
    val city = if (args.last.startsWith("city")) args.last.split(":").last else ""
    val fields = schemaFile.getLines().map(_.trim.split("\\s+")).map(columns => (columns(0), columns(1))).toList
    val dateBegin = date.split(",").head
    val dateEnd = date.split(",").last

    schemaFile.close()

    val valueKeys = 1.to(52)
    val begin = Calendar.getInstance()
    val end = Calendar.getInstance()
    begin.setTime(dateFmt.parse(dateBegin))
    end.setTime(dateFmt.parse(dateEnd))

    val sc = new SparkContext()
    val hive = new HiveContext(sc)
    val sectorMap = generateSectorMap(hive, "20190501", city)

    val data = mutable.HashMap[(String, String), Array[Float]]()
    val datePairList = new Array[(String, String)](valueKeys.length)
    0.to(51).foreach(i => {
      datePairList(i) = (dateFmt.format(begin.getTime), dateFmt.format(end.getTime))
      val calendar = Calendar.getInstance()
      calendar.setTime(begin.getTime)

      val dateHourList = mutable.ListBuffer[String]()
      while (calendar.before(end)) {
        0.to(23).foreach(hh => dateHourList.append(s"${dateFmt.format(calendar.getTime)}${fmt2Bit(hh)}"))
        calendar.add(Calendar.DATE, 1)
      }
      0.to(23).foreach(hh => dateHourList.append(s"${dateFmt.format(calendar.getTime)}${fmt2Bit(hh)}"))

      val weekData = generateKpiData(sc, dateHourList, fields, field, sectorMap)
      weekData.foreach(kv => {
        val (key, rows) = kv
        val hourValueMap = rows.map(row => (row.getString(2), row.getFloat(3))).toMap
        val sheet = Array.ofDim[Float](7, 24)
        val calendar = Calendar.getInstance()
        calendar.setTime(begin.getTime)
        0.to(6).foreach(day => {
          0.to(23).foreach(hour => {
            val hourKey = s"${dateFmt.format(calendar.getTime)}${fmt2Bit(hour)}"
            if (hourValueMap.contains(hourKey)) {
              sheet(day)(hour) = hourValueMap(hourKey)
            } else {
              sheet(day)(hour) = -1
            }
          })
          calendar.add(Calendar.DATE, 1)
        })
        // 每个时间点至少一条数据
        /*if (sheet.count(_.sum >= 1) == 24) {
          if (!data.contains(key))
            data(key) = new Array[Float](52)
          data(key)(i) = hourValue.values.max
        }
        // 每周至少一条数据
        if (sheet.count(_.sum >= 1) >= 1) {
          if (!data.contains(key))
            data(key) = new Array[Float](52)
          data(key)(i) = hourValue.values.max
        }*/
        // 取12个点有数的天的最大值的均值
        val subSheet = sheet.filter(_.count(_ == -1) <= 12)
        if (subSheet.length >= 3) {
          if (!data.contains(key))
            data(key) = Array.fill(52)(-1)
          data(key)(i) = subSheet.map(_.max).sum / subSheet.length
        }
      })
      begin.add(Calendar.DATE, 7)
      end.add(Calendar.DATE, 7)
    })

    val sqlClause = mutable.ListBuffer[String]()
    data.foreach(kv => {
      val raw = generateSectorInfo(sectorMap(kv._1))
      raw.append(s"'${datePairList.head._1},${datePairList.head._2}'")
      raw.append(s"'${datePairList.last._1},${datePairList.last._2}'")
      val values = data(kv._1)
      values.foreach(value => {
        if (value == -1)
          raw.append(null)
        else
          raw.append(value.toString)
      })

      val realValues = values.filter(_ > -1)
      val min = realValues.min
      val max = realValues.max

      raw.appendAll(baseCoefficient(values, valueKeys))
      raw.appendAll(rankAndTide(datePairList.indices.map(i => ((i + 1).toString, datePairList(i))).toMap, realValues))

      val coefficientMap = splitValues(values.min, values.max, values, valueKeys)
      raw.appendAll(coefficientScore(coefficientMap, valueKeys))

      // 波峰
      val (maxPeriod, coefficientPeakList) = coefficientPeak(coefficientMap, valueKeys)
      raw.appendAll(coefficientPeakList)
      // 波谷
      val (minPeriod, coefficientThroughList) = coefficientThrough(coefficientMap, valueKeys)
      raw.appendAll(coefficientThroughList)

      val (coefficientLimit, condition, valueLimit, minSize) = (0.1f, "value", 0.2f, 3)
      raw.appendAll(top3NearStable(false, values.max, minPeriod, min, coefficientMap, valueKeys, coefficientLimit, condition, valueLimit, minSize))
      raw.appendAll(top3NearStable(true, values.max, maxPeriod, min, coefficientMap, valueKeys, coefficientLimit, condition, max * .75f, minSize))

      sqlClause.append(s"(${raw.mkString(",").replaceAll("'null'", "null")})")
    })

    // mysql
    Class.forName(mysqlDriver)
    val mysqlConnect = DriverManager.getConnection(mysqlJdbcUrl)
    val mysqlStmt = mysqlConnect.createStatement()
    mysqlConnect.setAutoCommit(false)

    val pageSize = 2000
    val pageCount = sqlClause.length / pageSize
    0.to(pageCount).foreach(page => {
      val sql = s"insert into `$table` values ${sqlClause.slice(page * pageSize, (page + 1) * pageSize).mkString(",")}"
      mysqlStmt.execute(sql)
      mysqlConnect.commit()
    })

    mysqlStmt.close()
    mysqlConnect.close()
    sc.stop()
  }

  def rankAndTide(weekMap: Map[String, (String, String)], values: Seq[Float]): Seq[String] = {
    if (values.length == 52) {
      val ranks = mutable.ListBuffer[Int]()
      ranks.appendAll(values.map(value => {
        if (value <= 0.1)
          0
        else if (value <= 0.75)
          1
        else
          2
      }))
      val continueMap = gatherContinueFlags(1.to(52), ranks)
      val row = mutable.ListBuffer[String]()
      row.appendAll(ranks.map(_.toString))
      row.append(continueMap.size.toString)
      continueMap.slice(0, 12).foreach(kv => {
        val (key, rank) = kv
        val keyHead = key.split(":").head
        val keyTail = key.split(":").last
        val subValues = if (keyHead.toInt <= keyTail.toInt) {
          values.slice(keyHead.toInt - 1, keyTail.toInt)
        } else {
          values.slice(0, keyTail.toInt) ++ values.slice(keyHead.toInt - 1, 52)
        }
        row.append(s"'$keyHead-$keyTail[${weekMap(keyHead)._1}-${weekMap(keyTail)._2}],$rank,${subValues.sum / subValues.length}'")
      })
      1.to(12 - continueMap.size).foreach(_ => row.append(null))
      row
    } else {
      List.fill(65)(null)
    }
  }

  def main01(args: Array[String]): Unit = {
    val columns = mutable.ListBuffer[String]()
    val sqlFile = Source.fromFile("E:\\work\\zhc\\5gai\\src\\main\\resources\\sql\\WID_DWPRB_YEAR_mysql.sql")
    sqlFile.getLines().slice(79, 173).foreach(line => {
      columns.append(line.trim.split("\\s+").head)
    })
    sqlFile.close()

    // mysql
    Class.forName(mysqlDriver)
    val mysqlConnect = DriverManager.getConnection(mysqlJdbcUrl)
    val mysqlStmt = mysqlConnect.createStatement()
    mysqlConnect.setAutoCommit(false)

    val table = "wid_dwprb_year"
    val countRs = mysqlStmt.executeQuery(s"SELECT COUNT(*) FROM `$table`")
    val count = if (countRs.next()) countRs.getLong(1) else 0
    countRs.close()

    val pageSize = 100
    val pageCount = count / pageSize
    1.to(pageCount.toInt + 1).foreach(index => {
      val offset = (index - 1) * pageSize
      val sqlList = mutable.ListBuffer[String]()
      val rs = mysqlStmt.executeQuery(s"SELECT * FROM `$table` ORDER BY base_statn_id, cell_id LIMIT $offset, $pageSize")
      while (rs.next()) {
        val base_statn_id = rs.getInt(2)
        val cell_id = rs.getInt(4)
        val values = mutable.ListBuffer[Float]()
        14.to(65).foreach(i => {
          values.append(rs.getFloat(i))
        })
        if (values.count(_ > 0) == 52) {
          // TODO
          val row = calculateCoefficient(values, 0.1f, "value", 0.2f, 3)
          val sqlClause = mutable.ListBuffer[String]()
          columns.indices.foreach(i => {
            sqlClause.append(s"${columns(i)} = ${row(i)}")
          })
          val sql = s"update `$table` set ${sqlClause.mkString(",")} where base_statn_id = $base_statn_id and cell_id = $cell_id"
          sqlList.append(sql)
        }
      }
      rs.close()
      sqlList.foreach(mysqlStmt.executeUpdate)
      mysqlConnect.commit()
    })

    mysqlStmt.close()
    mysqlConnect.close()
  }

  def calculateCoefficient(values: Seq[Float], coefficientLimit: Float, condition: String, valueLimit: Float, minSize: Int): Seq[String] = {
    val valueKeys = 1.to(52)
    val row = mutable.ListBuffer[String]()
    val min = values.min
    val max = values.max

    val coefficientMap = splitValues(values.min, values.max, values, valueKeys)
    row.appendAll(coefficientScore(coefficientMap, valueKeys))

    // 波峰
    val (maxPeriod, coefficientPeakList) = coefficientPeak(coefficientMap, valueKeys)
    row.appendAll(coefficientPeakList)
    // 波谷
    val (minPeriod, coefficientThroughList) = coefficientThrough(coefficientMap, valueKeys)
    row.appendAll(coefficientThroughList)

    row.appendAll(top3NearStable(false, values.max, minPeriod, min, coefficientMap, valueKeys, coefficientLimit, condition, valueLimit, minSize))
    row.appendAll(top3NearStable(true, values.max, maxPeriod, min, coefficientMap, valueKeys, coefficientLimit, condition, max * .75f, minSize))
    row
  }
}
