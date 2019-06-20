package com.iss

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object WirelessKpi {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val dateFmt = new SimpleDateFormat("yyyyMMdd")
  val dateHourFmt = new SimpleDateFormat("yyyyMMddHH")

  def main(args: Array[String]): Unit = {
    val city = args(0)
    val category = args(1).toLowerCase()
    val date = args(2).replaceAll("-", "")
    val kpiPath = args(3)
    val field = args(4)

    val context = new SparkContext()
    val hiveContext = new HiveContext(context)

    val rows = if ("day" == category) {
      dayKpi(Array(city, date, field), hiveContext)
    } else if ("week" == category) {
      weekKpi(Array(city, date, field, args(5)), hiveContext)
    } else {
      combinationKpi(Array(city, date, field, args(5)), hiveContext)
    }

    if (rows.nonEmpty) {
      val partition = date.split(",").head
      context.parallelize(rows).repartition(4).saveAsTextFile(s"$kpiPath/$partition")

      hiveContext.sql("set hive.exec.dynamic.partition=true")
      hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      hiveContext.sql("use noce")
      hiveContext.sql(s"Alter table ${kpiPath.split("/").last} add IF NOT EXISTS partition(day=$partition) location '$partition/'")
    }

    context.stop()
  }

  def combinationKpi(args: Array[String], hiveContext: HiveContext): Seq[String] = {
    val city = args(0)
    val date = args(1)
    val field = args(2)

    val dayKpiMap = mutable.HashMap[String, mutable.ListBuffer[Float]]()
    val cellMap = mutable.HashMap[String, (String, String, String, String, String, String, String, Long, Byte, Int)]()
    0.to(23).foreach(i => {
      val hour = s"$date${if (i < 10) s"0$i" else i}"
      val hourKpiList = try {
        val sql =
          s"""
             |select base_statn_id, base_statn_name, cell_id, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width, $field
             |from AGG_WIRELESS_KPI_CELL_H a, DIM_SECTOR b
             |where a.enodebid = b.base_statn_id and a.cellid = b.cell_id
             |and b.day = $date and b.city_id = $city
             |and a.hour = $hour
             |and a.$field is not NULL and b.base_statn_id is not NULL and b.cell_id is not NULL
            """.stripMargin.split("\\s+").map(_.trim).filter(_.length > 0).mkString(" ")

        hiveContext.sql("use noce")
        hiveContext.sql(sql).collect()
      } catch {
        case e: Exception => {
          logger.error(e.getMessage)
          Array[Row]()
        }
      }

      hourKpiList.foreach(row => {
        val base_statn_id = row.getInt(0)
        val base_statn_name = row.getString(1)
        val cell_id = row.getInt(2)
        val cell_name = row.getString(3)
        val bs_vendor = row.getString(4)
        val is_indoor = row.getString(5)
        val band = row.getString(6)
        val system_type_or_standard = row.getString(7)
        val region = row.getString(8)
        val attribute = try {
          row.getLong(9)
        } catch {
          case e: Exception => {
            -99
          }
        }
        val nb_flag: Byte = try {
          row.getByte(10)
        } catch {
          case e: Exception => {
            -99
          }
        }
        val band_width = try {
          row.getInt(11)
        } catch {
          case e: Exception => {
            -99
          }
        }

        try {
          val fieldValue = row.getFloat(12)

          val key = s"$base_statn_id|$cell_id"
          if (dayKpiMap.contains(key)) {
            dayKpiMap(key).append(fieldValue)
          } else {
            dayKpiMap(key) = mutable.ListBuffer[Float](fieldValue)
          }

          cellMap(key) = (base_statn_name, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width)
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
          }
        }
      })
    })

    val rows = mutable.ListBuffer[String]()
    dayKpiMap.foreach(kv => {
      val (key, kpiList) = kv
      val row = mutable.ListBuffer[String](city, key.split("\\|").head, cellMap(key)._1, key.split("\\|").last, cellMap(key)._2,
        cellMap(key)._3, cellMap(key)._4, cellMap(key)._5, cellMap(key)._6, cellMap(key)._7, cellMap(key)._8.toString, cellMap(key)._9.toString, cellMap(key)._10.toString, kpiList.mkString("|"))

      rows.append(row.mkString("|"))
      logger.info(s"[$key]:[${row.mkString("|")}]")
    })
    rows
  }

  def weekKpi(args: Array[String], hiveContext: HiveContext): Seq[String] = {
    val city = args(0)
    val date = args(1)
    val field = args(2)
    val table = args(3)
    val dateStart = date.split(",").head

    /*val calendar = Calendar.getInstance()
    calendar.setTime(dateFmt.parse(dateStart))
    val offset = calendar.get(Calendar.DAY_OF_WEEK)*/

    val weekKpiMap = mutable.HashMap[String, mutable.ListBuffer[(Float, Float, Float, Float, Float, Float, Float, String, String, Float, String, String)]]()
    val cellMap = mutable.HashMap[String, (String, String, String, String, String, String, String, Long, Byte, Int)]()
    date.split(",").foreach(day => {
      val dayKpiList = try {
        val sql =
          s"""
             |select base_statn_id, base_statn_name, cell_id, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width,
             |${field}min, ${field}max, ${field}sum, ${field}mean, ${field}median, ${field}var, ${field}std, ${field}idxmax, ${field}idxmin, ${field}cv, trough
             |from $table where day = $day
          """.stripMargin.split("\\s+").map(_.trim).filter(_.length > 0).mkString(" ")

        hiveContext.sql("use noce")
        hiveContext.sql(sql).collect()
      } catch {
        case e: Exception => {
          logger.error(e.getMessage)
          Array[Row]()
        }
      }

      dayKpiList.foreach(row => {
        val base_statn_id = row.getInt(0)
        val base_statn_name = row.getString(1)
        val cell_id = row.getInt(2)
        val cell_name = row.getString(3)
        val bs_vendor = row.getString(4)
        val is_indoor = row.getString(5)
        val band = row.getString(6)
        val system_type_or_standard = row.getString(7)
        val region = row.getString(8)
        val attribute = row.getLong(9)
        val nb_flag = row.getByte(10)
        val band_width = row.getInt(11)

        val fieldmin = row.getFloat(12)
        val fieldmax = row.getFloat(13)
        val fieldsum = row.getFloat(14)
        val fieldmean = row.getFloat(15)
        val fieldmedian = row.getFloat(16)
        val fieldvar = row.getFloat(17)
        val fieldstd = row.getFloat(18)

        val fieldxmax = row.getString(19)
        val fieldxmin = row.getString(20)

        val fieldcv = row.getFloat(21)

        // 波谷时段
        val trough = row.getString(22)

        val key = s"$base_statn_id|$cell_id"
        cellMap(key) = (base_statn_name, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width)

        if (day == dateStart) {
          weekKpiMap(key) = mutable.ListBuffer((fieldmin, fieldmax, fieldsum, fieldmean, fieldmedian, fieldvar, fieldstd, fieldxmax, fieldxmin, fieldcv, trough, day))
        } else {
          if (weekKpiMap.contains(key))
            weekKpiMap(key).append((fieldmin, fieldmax, fieldsum, fieldmean, fieldmedian, fieldvar, fieldstd, fieldxmax, fieldxmin, fieldcv, trough, day))
        }
      })
    })

    // 调试
    /*weekKpiMap.filter(_._2.length >= 5).foreach(kv => {
      logger.info(s"[${kv._2.map(_._12).mkString(",")}]")
    })*/

    val rows = mutable.ListBuffer[String]()
    weekKpiMap.filter(_._2.length == 7).foreach(kv => {
      val (key, kpiList) = kv
      val idleflag = kpiList.map(_._11).contains("")
      if (idleflag) {
        logger.warn(s"[$table]:[$field]:[$key]:[${kpiList.map(_._11).mkString(",")}]")
      }
      val fieldMin = kpiList.minBy(_._1)._1
      val fieldMax = kpiList.maxBy(_._2)._2
      val meanMin = kpiList.minBy(_._4)._4
      val meanMax = kpiList.maxBy(_._4)._4
      val mean = kpiList.map(_._4).sum / kpiList.length
      val variance = kpiList.map(kpi => math.pow(kpi._4 - mean, 2)).sum / kpiList.length
      val std = math.sqrt(variance)

      val row = mutable.ListBuffer[String](
        city, key.split("\\|").head, cellMap(key)._1, key.split("\\|").last, cellMap(key)._2,
        cellMap(key)._3, cellMap(key)._4, cellMap(key)._5, cellMap(key)._6, cellMap(key)._7, cellMap(key)._8.toString, cellMap(key)._9.toString, cellMap(key)._10.toString,
        kpiList.map(kpi => {
          s"${kpi._1}|${kpi._2}|${kpi._3}|${kpi._4}|${kpi._5}|${kpi._6}|${kpi._7}|${kpi._8}|${kpi._9}|${kpi._10}|${kpi._11}"
        }).mkString("|"),
        fieldMin.toString,
        fieldMax.toString,
        kpiList.map(_._3).sum.toString,
        mean.toString,
        kpiList.map(_._4).sorted.toList(kpiList.length / 2).toString,
        variance.toString,
        std.toString,
        (kpiList.indexWhere(_._4 == meanMax) + 1).toString,
        (kpiList.indexWhere(_._4 == meanMin) + 1).toString,
        (std / mean).toString
      )

      val kpiFlagList = kpiList.map(kpi => {
        val quotient = (kpi._4 - meanMin) / (meanMax - meanMin)
        if (quotient <= 0.25)
          -1
        else if (quotient > 0.25 && quotient <= 0.75)
          0
        else
          1
      })
      kpiFlagList.foreach(flag => row.append(flag.toString))

      if ("maxrrc" == field || "maxact" == field) {
        row.append(kpiList.count(_._2 == 0).toString)
      }

      if (!idleflag) {
        val cal = categoryByWeek(kpiFlagList)
        row.append(cal.count(_._2 < 0).toString)

        val troughList = troughByWeek(cal)
        row.appendAll(troughList)

        if (troughList.length == 2)
          row.append(null)

        val troughHourList = mutable.ListBuffer[String]()
        troughList.foreach(weekday => {
          val weekdayHead = weekday.substring(0, 1).toInt
          val weekdayTail = weekday.substring(2).toInt
          val subTroughList = mutable.ListBuffer[String]()
          if (weekdayHead < weekdayTail) {
            weekdayHead.to(weekdayTail).foreach(i => subTroughList.append(kpiList(i - 1)._11))
          } else if (weekdayHead == weekdayTail) {
            subTroughList.append(kpiList(weekdayHead - 1)._11)
          } else {
            weekdayHead.to(7).foreach(i => subTroughList.append(kpiList(i - 1)._11))
            1.to(weekdayHead).foreach(i => subTroughList.append(kpiList(i - 1)._11))
          }
          troughHourList.appendAll(subTroughList)
          row.append(publicHour(subTroughList))
        })

        if (troughList.length == 2)
          row.append(null)

        row.append(publicHour(troughHourList))
      }

      rows.append(row.mkString("|"))
      logger.info(s"[$key]:[${row.mkString("|")}]:[${kpiList.map(_._12).mkString(",")}]")
    })
    rows
  }

  val publicHour = (subTroughList: Seq[String]) => {
    if (subTroughList.length == 1)
      subTroughList.head
    else {
      val hourStart = subTroughList.map(_.substring(0, 2).toInt).max
      val hourEnd = subTroughList.map(_.substring(3).toInt).min
      s"${if (hourStart < 10) s"0$hourStart" else hourStart}:${if (hourEnd < 10) s"0$hourEnd" else hourEnd}"
    }
  }

  val categoryByWeek = (kpiFlagList: Seq[Int]) => {
    val timeFlags = mutable.LinkedHashMap[String, Int]()
    1.to(7).foreach(i => {
      timeFlags(i.toString) = kpiFlagList(i - 1)
    })

    val cal = mutable.LinkedHashMap[String, Int]()
    val keys = mutable.ListBuffer[String]()
    var i = 1
    var j = 2
    while (i <= 7) {
      keys.append(i.toString)
      breakable {
        while (j <= 7) {
          if (timeFlags(j.toString) != timeFlags(i.toString)) {
            keys.append((j - 1).toString)
            i = j
            j = j + 1
            break
          }
          j = j + 1
        }
        if (j == 8) {
          i = j
          keys.append((j - 1).toString)
        } else
          i = i + 1
      }

      cal(keys.mkString(":")) = timeFlags((i - 1).toString)
      keys.clear()
    }

    // 合并头尾
    if (cal.size > 1 && cal.head._2 == cal.last._2) {
      val headKey = cal.head._1
      val tailKey = cal.last._1

      val flag = cal.last._2
      val headValue = (headKey.substring(2).toInt - headKey.substring(0, 1).toInt + 1) * flag
      val tailValue = (tailKey.substring(2).toInt - tailKey.substring(0, 1).toInt + 1) * flag
      val newValue = headValue + tailValue

      val newKey = s"${tailKey.substring(0, 1)}:${headKey.substring(2)}"

      cal.remove(headKey)
      cal.remove(tailKey)
      cal(newKey) = newValue
    }

    cal.foreach(kv => {
      val (key, flag) = kv
      if (flag.abs == 1) {
        val newValue = (key.substring(2).toInt - key.substring(0, 1).toInt + 1) * flag
        cal.update(key, newValue)
      }
    })

    cal
  }

  val troughByWeek = (cal: mutable.LinkedHashMap[String, Int]) => {
    val concaveList = cal.filter(_._2 < 0)
    val peakList = cal.filter(_._2 > 0)
    val zeroList = cal.filter(_._2 == 0)

    val troughList = mutable.ListBuffer[String]()
    if (concaveList.nonEmpty) {
      val minimum = concaveList.values.min
      troughList.append(concaveList.filter(_._2 == minimum).head._1)
    }

    if (peakList.nonEmpty) {
      val maximum = peakList.values.max
      troughList.append(peakList.filter(_._2 == maximum).head._1)
    }

    if (zeroList.nonEmpty) {
      zeroList.foreach(kv => {
        val (key, _) = kv
        val keyHead = key.substring(0, 1).toInt
        val keyTail = key.substring(2).toInt
        val newValue =
          if (keyHead <= keyTail)
            keyTail - keyHead + 1
          else
            7 - (keyHead - keyTail) + 1
        zeroList.update(key, newValue)
      })
      val maximum = zeroList.values.max
      troughList.append(zeroList.filter(_._2 == maximum).head._1)
    }
    // 最长波谷 周几到周几
    // 最长波峰 周几到周几
    // 最长0 周几到周几
    troughList
  }

  def dayKpi(args: Array[String], hiveContext: HiveContext): Seq[String] = {
    val city = args(0)
    val date = args(1)
    val field = args(2)

    val dayKpiMap = mutable.HashMap[String, mutable.ListBuffer[Float]]()
    val cellMap = mutable.HashMap[String, (String, String, String, String, String, String, String, Long, Byte, Int)]()
    0.to(23).foreach(i => {
      val hour = s"$date${if (i < 10) s"0$i" else i}"
      val hourKpiList = try {
        val sql =
          s"""
             |select base_statn_id, base_statn_name, cell_id, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width, $field
             |from AGG_WIRELESS_KPI_CELL_H a, DIM_SECTOR b
             |where a.enodebid = b.base_statn_id and a.cellid = b.cell_id
             |and b.day = $date and b.city_id = $city
             |and a.hour = $hour
             |and a.$field is not NULL and b.base_statn_id is not NULL and b.cell_id is not NULL
            """.stripMargin.split("\\s+").map(_.trim).filter(_.length > 0).mkString(" ")

        hiveContext.sql("use noce")
        hiveContext.sql(sql).collect()
      } catch {
        case e: Exception => {
          logger.error(e.getMessage)
          Array[Row]()
        }
      }

      hourKpiList.foreach(row => {
        val base_statn_id = row.getInt(0)
        val base_statn_name = row.getString(1)
        val cell_id = row.getInt(2)
        val cell_name = row.getString(3)
        val bs_vendor = row.getString(4)
        val is_indoor = row.getString(5)
        val band = row.getString(6)
        val system_type_or_standard = row.getString(7)
        val region = row.getString(8)
        val attribute = try {
          row.getLong(9)
        } catch {
          case e: Exception => {
            -99
          }
        }
        val nb_flag: Byte = try {
          row.getByte(10)
        } catch {
          case e: Exception => {
            -99
          }
        }
        val band_width = try {
          row.getInt(11)
        } catch {
          case e: Exception => {
            -99
          }
        }

        try {
          val fieldValue = row.getFloat(12)

          val key = s"$base_statn_id|$cell_id"
          if (dayKpiMap.contains(key)) {
            dayKpiMap(key).append(fieldValue)
          } else {
            dayKpiMap(key) = mutable.ListBuffer[Float](fieldValue)
          }

          cellMap(key) = (base_statn_name, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width)
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
          }
        }
      })

      dayKpiMap.filter(_._2.length != i + 1).keys.foreach(dayKpiMap.remove)
    })

    val rows = mutable.ListBuffer[String]()
    dayKpiMap.foreach(kv => {
      val (key, kpiList) = kv
      val row = mutable.ListBuffer[String](city, key.split("\\|").head, cellMap(key)._1, key.split("\\|").last, cellMap(key)._2,
        cellMap(key)._3, cellMap(key)._4, cellMap(key)._5, cellMap(key)._6, cellMap(key)._7, cellMap(key)._8.toString, cellMap(key)._9.toString, cellMap(key)._10.toString, kpiList.mkString("|"))

      val kpiMin = kpiList.min
      val kpiMax = kpiList.max

      row.append(kpiMin.toString)
      row.append(kpiMax.toString)
      row.append(kpiList.sum.toString)

      val mean = kpiList.sum / kpiList.length
      row.append(mean.toString)
      row.append(kpiList.sorted.toList(kpiList.length / 2).toString)

      val variance = kpiList.map(kpi => math.pow(kpi - mean, 2)).sum / kpiList.length
      val std = math.sqrt(variance)
      row.append(variance.toString)
      row.append(std.toString)

      val kpiMaxIndex = kpiList.indexWhere(_ == kpiMax)
      val kpiMinIndex = kpiList.indexWhere(_ == kpiMin)
      row.append(if (kpiMaxIndex < 10) s"0$kpiMaxIndex" else s"$kpiMaxIndex")
      row.append(if (kpiMinIndex < 10) s"0$kpiMinIndex" else s"$kpiMinIndex")

      row.append((std / mean).toString)

      val kpiFlagList = kpiList.map(kpi => if ((kpi - kpiMin) / (kpiMax - kpiMin) <= 0.25) -1 else 1)
      kpiFlagList.foreach(flag => row.append(flag.toString))

      if ("counter0003" == field || "counter0013" == field) {
        row.append(if (kpiMax == 0) "1" else "0")
      }

      row.append(trough(kpiFlagList))

      rows.append(row.mkString("|"))
      logger.info(s"[$key]:[${row.mkString("|")}]")
    })
    rows
  }

  val fmt24Hour = (i: Int) => s"${if (i < 10) "0" else ""}$i"

  // 最长谷起始时间
  val trough = (kpiFlagList: Seq[Int]) => {
    val timeFlags = mutable.LinkedHashMap[String, Int]()
    0.to(23).foreach(i => {
      timeFlags(fmt24Hour(i)) = kpiFlagList(i)
    })
    val positiveSum = timeFlags.filter(_._2 > 0).values.sum
    val negativeSum = timeFlags.filter(_._2 < 0).values.sum

    val cal = mutable.LinkedHashMap[String, Int]()
    if (positiveSum == 0) {
      // 全谷
      cal("00:23") = -1
    } else if (positiveSum < kpiFlagList.length) {
      val keys = mutable.ListBuffer[String]()
      var i = 0
      var j = 1
      while (i <= 23) {
        keys.append(fmt24Hour(i))
        breakable {
          while (j <= 23) {
            if (timeFlags(fmt24Hour(j)) != timeFlags(fmt24Hour(i))) {
              keys.append(fmt24Hour(j - 1))
              i = j
              j = j + 1
              break
            }
            j = j + 1
          }
          if (j == 24) {
            i = j
            keys.append(fmt24Hour(j - 1))
          } else
            i = i + 1
        }

        cal(keys.mkString(":")) = timeFlags(fmt24Hour(i - 1))
        keys.clear()
      }
    } else {
      // 全峰
      cal("00:23") = 1
    }

    // 合并头尾
    if (cal.size > 1 && cal.head._2 == cal.last._2) {
      val headKey = cal.head._1
      val tailKey = cal.last._1

      val flag = cal.last._2
      val headValue = (headKey.substring(3).toInt - headKey.substring(0, 2).toInt + 1) * flag
      val tailValue = (tailKey.substring(3).toInt - tailKey.substring(0, 2).toInt + 1) * flag
      val newValue = headValue + tailValue

      val newKey = s"${tailKey.substring(0, 2)}:${headKey.substring(3)}"

      cal.remove(headKey)
      cal.remove(tailKey)
      cal(newKey) = newValue
    }

    cal.foreach(kv => {
      val (key, flag) = kv
      if (flag.abs == 1) {
        val newValue = (key.substring(3).toInt - key.substring(0, 2).toInt + 1) * flag
        cal.update(key, newValue)
      }
    })

    // logger.info(timeFlags.values.mkString("\t"))
    // logger.info(s"峰谷概览: ${cal.mkString(", ")}")
    // logger.info(s"峰小时数: $positiveSum")
    // logger.info(s"谷小时数: ${negativeSum.abs}")

    // val peakList = cal.filter(_._2 > 0)
    val concaveList = cal.filter(_._2 < 0)

    if (concaveList.nonEmpty) {
      val minimum = concaveList.values.min
      concaveList.filter(_._2 == minimum).head._1
    } else {
      ""
    }

    // logger.info(s"峰: [${peakList.keys.mkString(",")}], ${peakList.size} 个")
    // logger.info(s"谷: [${concaveList.keys.mkString(",")}], ${concaveList.size} 个")

    /*val peakConcave = mutable.ListBuffer[String]()
    if (peakList.nonEmpty) {
      val maximum = peakList.maxBy(_._2)
      // logger.info(s"持续最长峰起止时间: [${peakList.filter(_._2 == maximum._2).keys.mkString(",")}], 持续 ${maximum._2} 个小时")
      peakConcave.append(peakList.filter(_._2 == maximum._2).head._1)
    }

    if (concaveList.nonEmpty) {
      val minimum = concaveList.minBy(_._2)
      // logger.info(s"持续最长谷起止时间: [${concaveList.filter(_._2 == minimum._2).keys.mkString(",")}], 持续 ${minimum._2.abs} 个小时")
      peakConcave.append(concaveList.filter(_._2 == minimum._2).head._1)
    }

    peakConcave.mkString(",")*/
  }
}
