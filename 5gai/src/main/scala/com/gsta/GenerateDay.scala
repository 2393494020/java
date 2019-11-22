package com.gsta

import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object GenerateDay extends Util with Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val date = args(0)
    val field = args(1)
    val coefficientLimit = args(2).toFloat
    val condition = args(3)
    val delta = args(4).toFloat
    val minSize = args(5).toInt
    val schemaFile = Source.fromFile(args(6))
    val fields = schemaFile.getLines().map(_.trim.split("\\s+")).map(columns => (columns(0), columns(1))).toList
    val savePath = args(7)
    val directList = savePath.split("/")
    val partition = directList.last
    val city = if (args.last.startsWith("city")) args.last.split(":").last else ""

    schemaFile.close()

    val valueKeys = 0.to(23)
    val calendar = Calendar.getInstance()
    calendar.setTime(dateFmt.parse(date))
    calendar.add(Calendar.DATE, -14)

    val day_of_week = dayOfWeek(calendar)
    val dateHourList = mutable.ListBuffer[String]()
    calendar.add(Calendar.HOUR, -1)
    1.to(15 * 24 + 2).foreach(_ => {
      dateHourList.append(dateHourFmt.format(calendar.getTime))
      calendar.add(Calendar.HOUR, 1)
    })
    val targetHourList = dateHourList.slice(dateHourList.length - 25, dateHourList.length - 1)
    val tweekHourList = dateHourList.slice(1, 14 * 24 + 1)

    // logger.info(tweekHourList.mkString(","))
    // logger.info(targetHourList.mkString(","))
    val sc = new SparkContext()
    val hive = new HiveContext(sc)

    val rows = mutable.ListBuffer[String]()

    val sectorMap = generateSectorMap(hive, "20190501", city)
    val dataMap = generateKpiData(sc, dateHourList, fields, field, sectorMap)
    dataMap.foreach(kv => {
      val dateHourDataMap = kv._2.map(row => (row.getString(2), row.getFloat(3))).toMap
      val fillValues = mutable.ListBuffer[Float]()
      valueKeys.foreach(i => {
        val dateHour = targetHourList(i)
        if (dateHourDataMap.contains(dateHour)) {
          fillValues.append(dateHourDataMap(dateHour))
        } else {
          // 补值
          // 前后一小时均值
          val j = 14 * 24 + i + 1
          var mean = if (dateHourDataMap.contains(dateHourList(j - 1)) && dateHourDataMap.contains(dateHourList(j + 1))) {
            (dateHourDataMap(dateHourList(j - 1)) + dateHourDataMap(dateHourList(j + 1))) / 2
          } else {
            -1
          }

          /*
          // 1.前一周当前时刻
          // 2.前一周当前时刻的前后一小时
          val preWeek = j - 7 * 24
          val preWeekValue = if (dateHourDataMap.contains(hourList(preWeek))) {
            dateHourDataMap(hourList(preWeek))
          } else if (dateHourDataMap.contains(hourList(preWeek - 1)) && dateHourDataMap.contains(hourList(preWeek + 1))) {
            (dateHourDataMap(hourList(preWeek - 1)) + dateHourDataMap(hourList(preWeek + 1))) / 2
          } else {
            -1
          }

          // 1.前两周当前时刻
          // 2.前两周当前时刻的前后一小时
          val pre2Week = j - 14 * 24
          val pre2WeekValue = if (dateHourDataMap.contains(hourList(pre2Week))) {
            dateHourDataMap(hourList(pre2Week))
          } else if (dateHourDataMap.contains(hourList(pre2Week - 1)) && dateHourDataMap.contains(hourList(pre2Week + 1))) {
            (dateHourDataMap(hourList(pre2Week - 1)) + dateHourDataMap(hourList(pre2Week + 1))) / 2
          } else {
            -1
          }

          if (mean == -1 && preWeekValue != -1 && pre2WeekValue != -1) {
            mean = (preWeekValue + pre2WeekValue) / 2
          }*/

          fillValues.append(mean)
        }
      })
      val tweekValues = tweekHourList.filter(dateHour => dateHourDataMap.contains(dateHour)).map(dateHourDataMap(_))
      if (fillValues.count(_ == -1) < 24 && tweekValues.nonEmpty && tweekValues.max != fillValues.filter(_ > -1).min) {
        // 静态属性
        val row = generateSectorInfo(sectorMap(kv._1))
        // 前两周数据(非填充)
        // 原始数据
        targetHourList.foreach(dateHour => {
          if (dateHourDataMap.contains(dateHour)) {
            row.append(dateHourDataMap(dateHour).toString)
          } else {
            row.append("")
          }
        })

        val fullFlag = row.slice(row.length - valueKeys.length, row.length).count(x => x.nonEmpty)
        val fillFullFlag = fillValues.count(_ > -1)

        fillValues.foreach(x => if (x >= 0) row.append(x.toString) else row.append(""))
        row.append(fullFlag.toString)
        row.append(fillFullFlag.toString)
        row.appendAll(baseCoefficient(fillValues, valueKeys))

        val realValues = fillValues.filter(_ > 1)
        val coefficientMap = splitValues(tweekValues.min, tweekValues.max, fillValues, valueKeys)
        if (delta < 1) {
          // low1 ~ low7
          // dwprb
          row.appendAll(lowPeriod(fillValues))
        } else {
          // maxact
          row.appendAll(zeroLessActuser(fillValues, valueKeys, coefficientMap.toMap))
        }

        if (fillValues.length > realValues.length) {
          1.to(valueKeys.length + 26).foreach(_ => row.append(""))
        } else {
          row.appendAll(coefficientScore(coefficientMap, valueKeys))
          // 波峰
          val (_, coefficientPeakList) = coefficientPeak(coefficientMap, valueKeys)
          row.appendAll(coefficientPeakList)
          // 波谷
          val (minPeriod, coefficientThroughList) = coefficientThrough(coefficientMap, valueKeys)
          row.appendAll(coefficientThroughList)

          val min = realValues.min
          val valueLimit = if (delta < 1) min + (tweekValues.max - min) * delta else delta
          row.appendAll(top3NearStable(false, tweekValues.max, minPeriod, min, coefficientMap, valueKeys, coefficientLimit, condition, valueLimit, minSize))
        }
        row.append(day_of_week.toString)

        rows.append(row.mkString("|").replaceAll("'", "").replaceAll("null", ""))
      }
    })

    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    sc.parallelize(rows).repartition(1).saveAsTextFile(savePath)

    hive.sql("set hive.exec.dynamic.partition=true")
    hive.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hive.sql(s"alter table ${directList(directList.length - 2)} add IF NOT EXISTS partition(day=$partition) location '$partition/'")
  }

  def lowPeriod(values: Seq[Float]): Seq[String] = {
    val row = mutable.ListBuffer[String]()
    row.append(splitLowValues(1, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(2, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(3, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(4, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(5, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(6, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(7, values).filter(_._2 == 1).keys.mkString(","))
    row.append(splitLowValues(8, values).filter(_._2 == 1).keys.mkString(","))
    row
  }

  def splitLowValues(flag: Int, values: Seq[Float]): mutable.LinkedHashMap[String, Int] = {
    val flagList = 0.to(23).map(i => {
      val x = values(i)
      if ((flag == 1 && 0f < x && x <= 0.025f) ||
        (flag == 2 && 0f < x && x <= 0.05f) ||
        (flag == 3 && 0f < x && x <= 0.075f) ||
        (flag == 4 && 0f < x && x <= 0.1f) ||
        (flag == 5 && 0f < x && x <= 0.15f) ||
        (flag == 6 && 0f < x && x <= 0.2f) ||
        (flag == 7 && 0f < x && x <= 0.3f) ||
        (flag == 8 && 0.3f < x)) {
        1
      } else {
        0
      }
    })
    gatherContinueFlagsCircle(0.to(23), flagList)
  }

  def zeroLessActuser(values: Seq[Float], valueKeys: Seq[Int], coefficientMap: Map[String, (Float, Seq[Float])]): Seq[String] = {
    val headTail = s"${fmt2Bit(valueKeys.head)}:${fmt2Bit(valueKeys.last)}"
    val min = values.filter(_ > -1).min
    val row = mutable.ListBuffer[String]()
    // 零值时段
    if (min > 0)
      row.append("")
    else if (values.count(_ == 0) == values.length)
      row.append(headTail)
    else if (values.count(_ == 0) == 1)
      row.append(fmt2Bit(valueKeys(values.indexWhere(_ == 0))))
    else {
      // 如果波谷=零值，则取波谷
      val flagList = mutable.ListBuffer[Int]()
      valueKeys.indices.foreach(i => {
        if (values(i) == 0)
          flagList.append(0)
        else
          flagList.append(1)
      })
      val continueFlags = gatherContinueFlags(valueKeys, flagList).filter(_._2 == 0).map(kv => {
        val key = kv._1
        val subKeys = periodToList(key)
        (key, subKeys)
      })
      if (continueFlags.nonEmpty) {
        // val coefficientMap = splitValues(weekMin, weekMax, valueKeys, values)
        val coefficientTemp = coefficientMap.minBy(_._2._1)._2._1
        val coefficientMin = coefficientMap.filter(_._2._1 == coefficientTemp).maxBy(_._2._2.size)
        // val coefficientMin = coefficientMap.minBy(_._2._1)
        val troughKeys = periodToList(coefficientMin._1)

        val sortedContinueFlags = continueFlags.toList.sortWith((x, y) => x._2.size > y._2.size)
        val troughContainZeroContinue = sortedContinueFlags.filter(kv => {
          troughKeys.count(kv._2.contains(_)) == troughKeys.length
        })

        if (troughContainZeroContinue.nonEmpty) {
          row.append(troughContainZeroContinue.head._1)
        } else {
          row.append(sortedContinueFlags.head._1)
        }
      } else {
        row.append("")
      }
    }
    // 少用户时段
    if (min > 3) {
      row.append("")
    } else {
      val flagList = mutable.ListBuffer[Int]()
      valueKeys.indices.foreach(i => {
        if (0 < values(i) && values(i) <= 3)
          flagList.append(0)
        else
          flagList.append(1)
      })
      val continueFlags = gatherContinueFlags(valueKeys, flagList).filter(_._2 == 0).map(kv => {
        val key = kv._1
        (key, periodToList(key))
      }).toList.sortWith((x, y) => x._2.length > y._2.length)

      if (continueFlags.nonEmpty)
        row.append(continueFlags.head._1)
      else
        row.append("")
    }
    row
  }
}
