package com.gsta

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

trait Util {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val dateFmt = new SimpleDateFormat("yyyyMMdd")
  val dateHourFmt = new SimpleDateFormat("yyyyMMddHH")
  val user = "5gai"
  val password = "5G_ai_2019"
  val mysqlDriver = "com.mysql.jdbc.Driver"
  val mysqlJdbcUrl = s"jdbc:mysql://10.17.35.114:3306/5gai?user=$user&password=$password&useSSL=false&rewriteBatchedStatements=true"
  val dataDir = "/DATA/PUBLIC/NOCE/AGG/AGG_WIRELESS_KPI_CELL_H"

  def generateKpiData(sc: SparkContext, dateHourList: Seq[String], fields: Seq[(String, String)], field: String, sectorMap: collection.Map[(String, String), Row]): collection.Map[(String, String), Iterable[Row]] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    sc.union(
      dateHourList.filter(dateHour => fs.exists(new Path(s"$dataDir/hour=$dateHour"))).map(dateHour => {
        sc.textFile(s"$dataDir/hour=$dateHour").map(line => {
          val row = kpiTextLine2MapRow(line, fields)
          Row(row("enodebid"), row("cellid"), dateHour, row(field).toFloat)
        })
      })
    ).filter(row => {
      val enodebid = row.getString(0)
      val cellid = row.getString(1)
      val fieldValue = row.getFloat(3)
      isInteger(enodebid) && isInteger(cellid) && (if ("dw_prb_userate" == field || "up_prb_userate" == field) fieldValue > 0 else fieldValue >= 0) && sectorMap.contains((enodebid, cellid))
    }).groupBy(row => (row.getString(0), row.getString(1))).collectAsMap()
  }

  def generateSectorMap(hive: HiveContext, date: String, city: String): collection.Map[(String, String), Row] = {
    val dimSql = s"select distinct base_statn_id, base_statn_name, cell_id, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width, city_id from DIM_SECTOR where base_statn_name is not NULL and cell_name is not NULL and day = $date"
    hive.sql("use noce")
    hive.sql(if (city.isEmpty) dimSql else dimSql + s" and city_id = '$city'").map(row => ((row.getInt(0).toString, row.getInt(2).toString), row)).collectAsMap()
  }

  def generateSectorInfo(sector: Row): mutable.ListBuffer[String] = {
    val city = sector.getInt(12).toString
    val base_statn_id = sector.getInt(0).toString
    val base_statn_name = sector.getString(1)
    val cell_id = sector.getInt(2).toString
    val cell_name = sector.getString(3)
    val bs_vendor = sector.getString(4)
    val is_indoor = sector.getString(5)
    val band = sector.getString(6)
    val system_type_or_standard = sector.getString(7)
    val region = sector.getString(8)
    val attribute = try {
      sector.getLong(9).toString
    } catch {
      case _: Exception => null
    }
    val nb_flag = try {
      sector.getByte(10).toString
    } catch {
      case _: Exception => null
    }
    val band_width = try {
      sector.getInt(11).toString
    } catch {
      case _: Exception => null
    }

    mutable.ListBuffer[String](city, base_statn_id, s"'$base_statn_name'", cell_id, s"'$cell_name'", s"'$bs_vendor'", s"'$is_indoor'", s"'$band'", s"'$system_type_or_standard'", s"'$region'", attribute, nb_flag, band_width)
  }

  def kpiTextLine2MapRow(line: String, fields: Seq[(String, String)]): Map[String, String] = {
    val values = line.split(s"\\|", -1)
    val row = mutable.HashMap[String, String]()
    for (i <- fields.indices) {
      val (key, dataType) = fields(i)
      if (values.length <= i) {
        if ("string" == dataType)
          row(key) = ""
        else
          row(key) = "-1"
      } else {
        row(key) = if (values(i).trim == "\\N") "-1" else values(i).trim
      }
    }
    row.toMap
  }

  def increaseOrDecrease(values: Seq[Float]): Boolean = {
    // 顺序
    val ascSeq = values.sorted
    var asc = true
    values.indices.foreach(i => {
      if (ascSeq(i) != values(i))
        asc = false
    })

    if (asc) {
      asc
    } else {
      val descSeq = ascSeq.reverse
      var desc = true
      values.indices.foreach(i => {
        if (descSeq(i) != values(i))
          desc = false
      })
      desc
    }
  }

  def gatherContinueFlags(keyRange: Seq[Int], flagList: Seq[Int]): mutable.LinkedHashMap[String, Int] = {
    val continueFlagMap = mutable.LinkedHashMap[String, Int]()
    var prev = flagList.head
    var i = 0
    var j = 1
    while (j < flagList.length) {
      if (prev != flagList(j)) {
        continueFlagMap(s"${fmt2Bit(keyRange(i))}:${fmt2Bit(keyRange(j - 1))}") = prev
        prev = flagList(j)
        i = j
      }
      j = j + 1
    }
    continueFlagMap(s"${fmt2Bit(keyRange(i))}:${fmt2Bit(keyRange(j - 1))}") = prev
    if (logger.isDebugEnabled) {
      continueFlagMap.foreach(kv => logger.debug(s"${kv._1}->${kv._2}"))
    }
    continueFlagMap
  }

  def gatherContinueFlagsCircle(keyRange: Range, flagList: Seq[Int]): mutable.LinkedHashMap[String, Int] = {
    val continueFlagMap = gatherContinueFlags(keyRange, flagList)
    // 合并头尾
    if (continueFlagMap.size > 1 && continueFlagMap.head._2 == continueFlagMap.last._2) {
      val headKey = continueFlagMap.head._1
      val tailKey = continueFlagMap.last._1

      val flag = continueFlagMap.last._2
      val newKey = s"${tailKey.split(":").head}:${headKey.split(":").last}"

      continueFlagMap.remove(headKey)
      continueFlagMap.remove(tailKey)
      continueFlagMap(newKey) = flag
    }
    if (logger.isDebugEnabled) {
      logger.debug("合并头尾")
      continueFlagMap.foreach(kv => logger.debug(s"${kv._1}->${kv._2}"))
    }
    continueFlagMap
  }

  // 切分平衡周期
  def splitValues(weekMin: Float, weekMax: Float, values: Seq[Float], valueKeyRange: Seq[Int]): mutable.LinkedHashMap[String, (Float, Seq[Float])] = {
    val headTail = s"${fmt2Bit(valueKeyRange.head)}:${fmt2Bit(valueKeyRange.last)}"
    val max = values.max
    val min = values.min
    val continueDelta = (weekMax - min) * 0.05f
    val skipDelta = (weekMax - min) * 0.075f

    val rangeValuesMap = mutable.LinkedHashMap[String, Seq[Float]]()
    if (max - min <= continueDelta) {
      rangeValuesMap(headTail) = values
    } else {
      var i = 0
      var j = 1
      var prev = values.head
      while (j < values.length) {
        val next = values(j)
        val segments = values.slice(i, j)
        if (math.abs(next - prev) <= continueDelta && math.abs(next - segments.max) <= skipDelta && math.abs(next - segments.min) <= skipDelta) {
          prev = next
        } else {
          val key = s"${fmt2Bit(valueKeyRange(i))}:${fmt2Bit(valueKeyRange(j - 1))}"
          rangeValuesMap(key) = values.slice(i, j)
          prev = next
          i = j
        }
        j = j + 1
      }

      val key = s"${fmt2Bit(valueKeyRange(i))}:${fmt2Bit(valueKeyRange(j - 1))}"
      rangeValuesMap(key) = values.slice(i, j)
    }

    val finalRangeValuesMap = mutable.LinkedHashMap[String, Seq[Float]]()
    if (rangeValuesMap.size > 1) {
      var headKey = rangeValuesMap.head._1
      var tailKey = rangeValuesMap.last._1

      var headSubValues = rangeValuesMap.head._2
      var tailSubValues = rangeValuesMap.last._2

      // [00:06]:[0.0682,0.0591,0.0525,0.0478,0.0497,0.0501,0.0548]
      // [20:23]:[0.0999,0.0873,0.0867,0.0797]
      while (headSubValues.nonEmpty && math.abs(tailSubValues.last - headSubValues.head) <= continueDelta && math.abs(tailSubValues.max - headSubValues.head) <= skipDelta && math.abs(tailSubValues.min - headSubValues.head) <= skipDelta) {
        tailSubValues = tailSubValues :+ headSubValues.head
        headSubValues = headSubValues.slice(1, headSubValues.length)
        tailKey = s"${tailKey.split(":").head}:${headKey.split(":").head}"
        headKey = s"${fmt2Bit(headKey.split(":").head.toInt + 1)}:${headKey.split(":").last}"
      }

      if (headSubValues.isEmpty) {
        if (rangeValuesMap.size == 2) {
          rangeValuesMap.clear()
          rangeValuesMap(headTail) = values
        } else {
          val headKey = rangeValuesMap.head._1
          val lastKey = rangeValuesMap.last._1
          val tailKey = s"${lastKey.split(":").head}:${headKey.split(":").last}"
          val tailValues = rangeValuesMap.last._2 ++ rangeValuesMap.head._2
          rangeValuesMap(tailKey) = tailValues
          rangeValuesMap.remove(headKey)
          rangeValuesMap.remove(lastKey)
        }
        rangeValuesMap.foreach(kv => {
          finalRangeValuesMap(kv._1) = kv._2
        })
      } else {
        rangeValuesMap.remove(rangeValuesMap.head._1)
        rangeValuesMap.remove(rangeValuesMap.last._1)
        finalRangeValuesMap(headKey) = headSubValues
        rangeValuesMap.foreach(kv => {
          finalRangeValuesMap(kv._1) = kv._2
        })
        finalRangeValuesMap(tailKey) = tailSubValues
      }
    } else {
      rangeValuesMap.foreach(kv => {
        finalRangeValuesMap(kv._1) = kv._2
      })
    }

    if (logger.isDebugEnabled) {
      logger.debug(s"[${values.mkString(",")}]")
      logger.debug(s"[min]:[$min]")
      logger.debug(s"[max]:[$max]")
      logger.debug(s"[weekMin]:[$weekMin]")
      logger.debug(s"[weekMax]:[$weekMax]")
      finalRangeValuesMap.foreach(kv => {
        val (key, children) = kv
        val coefficient = (children.max - min) / (weekMax - min)
        logger.debug(s"[$key]:[$coefficient]:[${children.mkString(",")}]")
      })
    }

    finalRangeValuesMap.map(kv => {
      val (key, children) = kv
      val coefficient = (children.max - min) / (weekMax - min)
      (key, (coefficient, children))
    })
  }

  def baseCoefficient(values: Seq[Float], valueKeys: Seq[Int]): Seq[String] = {
    val row = mutable.ListBuffer[String]()
    val realValues = values.filter(_ > -1)
    val sum = if (values.count(_ == -1) > 0) 0 else values.sum
    val min = realValues.min
    val max = values.max

    row.append(min.toString)
    row.append(max.toString)
    row.append(if (sum == 0) "" else sum.toString)

    val mean = realValues.sum / realValues.length
    row.append(mean.toString)
    row.append(realValues.sorted.toList(realValues.length / 2).toString)

    val variance = realValues.map(x => math.pow(x - mean, 2)).sum / realValues.length
    val std = math.sqrt(variance)
    row.append(variance.toString)
    row.append(std.toString)

    val kpiMaxIndex = values.indexWhere(_ == max)
    val kpiMinIndex = values.indexWhere(_ == min)
    row.append(s"'${fmt2Bit(valueKeys(kpiMaxIndex))}'")
    row.append(s"'${fmt2Bit(valueKeys(kpiMinIndex))}'")

    row.append(if (mean == 0) "" else (std / mean).toString)
    row
  }

  def coefficientScore(coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys: Seq[Int]): Seq[String] = {
    if (coefficientMap.size == 1) {
      valueKeys.map(_ => coefficientMap.head._2._1.toString)
    } else {
      val divideCoefficientMap = mutable.HashMap[String, Float]()
      coefficientMap.foreach(kv => {
        val (period, (coefficient, _)) = kv
        val keys = periodToList(period, valueKeys.head, valueKeys.last)
        keys.foreach(key => {
          divideCoefficientMap(key.toString) = coefficient
        })
      })

      // 24 小时潮汐系数
      valueKeys.map(key => {
        divideCoefficientMap(key.toString).toString
      })
    }
  }

  def coefficientPeak(coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int]): (String, Seq[String]) = {
    val values = mutable.ListBuffer[String]()
    // 最大系数是为波峰
    val coefficientTemp = coefficientMap.maxBy(_._2._1)._2._1
    val coefficientMax = coefficientMap.filter(_._2._1 == coefficientTemp).maxBy(_._2._2.size)
    // 波峰
    values.append(s"'${coefficientMax._1}'")
    values.append((coefficientMax._2._2.sum / coefficientMax._2._2.length).toString)
    values.append(coefficientMax._2._2.max.toString)
    values.append(coefficientMax._2._1.toString)
    // 波峰核心周期
    val maxKey = coefficientMax._1
    val peakValues = coefficientMax._2._2
    val peakKeys = periodToList(maxKey, valueKeys.head, valueKeys.last)
    logger.debug(s"[波峰]:[$maxKey]:[${peakKeys.mkString(",")}]:[${peakValues.mkString(",")}]")

    val peakCore = calculateCore(peakValues, peakKeys)
    values.append(s"'$peakCore'")
    logger.debug(s"[波峰核心]:[$peakCore]")

    (maxKey, values)
  }

  def coefficientThrough(coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int]): (String, Seq[String]) = {
    val values = mutable.ListBuffer[String]()
    // 最小系数是为波谷
    val coefficientTemp = coefficientMap.minBy(_._2._1)._2._1
    val coefficientMin = coefficientMap.filter(_._2._1 == coefficientTemp).maxBy(_._2._2.size)
    // 波谷
    values.append(s"'${coefficientMin._1}'")
    values.append((coefficientMin._2._2.sum / coefficientMin._2._2.length).toString)
    values.append(coefficientMin._2._2.max.toString)
    values.append(coefficientMin._2._1.toString)
    // 波谷核心周期
    val minKey = coefficientMin._1
    val troughValues = coefficientMin._2._2
    val troughKeys = periodToList(minKey, valueKeys.head, valueKeys.last)
    logger.debug(s"[波谷]:[$minKey]:[${troughKeys.mkString(",")}]:[${troughValues.mkString(",")}]")

    val troughCore = calculateCore(troughValues, troughKeys)
    values.append(s"'$troughCore'")
    logger.debug(s"[波谷核心]:[$troughCore]")

    (minKey, values)
  }

  def nearStablePeak(coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int], coefficientLimit: Float, condition: String, valueLimit:Float, minSize: Int): Seq[(String, Seq[Float])] = {
    // 取大致平稳周期
    // 按潮汐系数,最大值,小时数过滤
    val nearStableCoefficientMap = if ("coefficient" == condition) {
      coefficientMap.filter(_._2._1 >= coefficientLimit)
    } else if ("value" == condition) {
      coefficientMap.filter(_._2._2.max >= valueLimit)
    } else if ("and" == condition) {
      coefficientMap.filter(kv => kv._2._1 >= coefficientLimit && kv._2._2.max >= valueLimit)
    } else {
      coefficientMap.filter(kv => kv._2._1 >= coefficientLimit || kv._2._2.max >= valueLimit)
    }

    if (logger.isDebugEnabled) {
      logger.debug(s"按潮汐系数:[$coefficientLimit],最大值:[$valueLimit]过滤")
      logger.debug(s"[时段]:[均值]:[最大值]:[系数]:[时段对应值]")
      nearStableCoefficientMap.foreach(kv => {
        val (key, (coefficient, children)) = kv
        val childMean = children.sum / children.length
        logger.debug(s"[$key]:[$childMean]:[${children.max}]:[$coefficient]:[${children.mkString(",")}]")
      })
    }
    gather(nearStableCoefficientMap, valueKeys).filter(_._2.size >= minSize).toList.sortWith((x, y) => x._2.size > y._2.size)
  }

  def nearStableThrough(coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int], coefficientLimit: Float, condition: String, valueLimit:Float, minSize: Int): Seq[(String, Seq[Float])] = {
    // 取大致平稳周期
    // 按潮汐系数,最大值,小时数过滤
    val nearStableCoefficientMap = if ("coefficient" == condition) {
      coefficientMap.filter(_._2._1 <= coefficientLimit)
    } else if ("value" == condition) {
      coefficientMap.filter(_._2._2.max <= valueLimit)
    } else if ("and" == condition) {
      coefficientMap.filter(kv => kv._2._1 <= coefficientLimit && kv._2._2.max <= valueLimit)
    } else {
      coefficientMap.filter(kv => kv._2._1 <= coefficientLimit || kv._2._2.max <= valueLimit)
    }

    if (logger.isDebugEnabled) {
      logger.debug(s"按潮汐系数:[$coefficientLimit],最大值:[$valueLimit]过滤")
      logger.debug(s"[时段]:[均值]:[最大值]:[系数]:[时段对应值]")
      nearStableCoefficientMap.foreach(kv => {
        val (key, (coefficient, children)) = kv
        val childMean = children.sum / children.length
        logger.debug(s"[$key]:[$childMean]:[${children.max}]:[$coefficient]:[${children.mkString(",")}]")
      })
    }
    gather(nearStableCoefficientMap, valueKeys).filter(_._2.size >= minSize).toList.sortWith((x, y) => x._2.size > y._2.size)
  }

  def gather(nearStableCoefficientMap:mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int]): Seq[(String, Seq[Float])] = {
    val headTail = s"${fmt2Bit(valueKeys.head)}:${fmt2Bit(valueKeys.last)}"
    // 01:04 -> (coefficient, [0.1, 0.2, 0.3, 0.4])
    // 05:06 -> (coefficient, [0.5, 0.6])
    // 合并后
    // 01:09 -> [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    val nearStableValueList = mutable.LinkedHashMap[String, Seq[Float]]()
    if (nearStableCoefficientMap.nonEmpty) {
      var prev = nearStableCoefficientMap.head._1.split(":").last.toInt
      var start = 0
      var index = 1
      nearStableCoefficientMap.slice(1, nearStableCoefficientMap.size).foreach(kv => {
        val current = kv._1.split(":").head.toInt
        if (current - prev > 1) {
          val subList = nearStableCoefficientMap.slice(start, index).toList
          val key = s"${subList.head._1.split(":").head}:${subList.last._1.split(":").last}"

          nearStableValueList(key) = subList.flatMap(_._2._2)
          start = index
        }
        prev = kv._1.split(":").last.toInt
        index = index + 1
      })

      val subList = nearStableCoefficientMap.slice(start, index).toList
      val key = s"${subList.head._1.split(":").head}:${subList.last._1.split(":").last}"
      nearStableValueList(key) = subList.flatMap(_._2._2)

      if (nearStableValueList.size > 1) {
        val headKey = nearStableValueList.head._1
        val tailKey = nearStableValueList.last._1
        if ((headKey.split(":").head == fmt2Bit(valueKeys.head) && tailKey.split(":").last == fmt2Bit(valueKeys.last))
          || (headKey.split(":").head.toInt - tailKey.split(":").last.toInt == 1)) {
          val newKey = s"${tailKey.split(":").head}:${headKey.split(":").last}"
          val subList = nearStableValueList.last._2 ++ nearStableValueList.head._2
          nearStableValueList.remove(headKey)
          nearStableValueList.remove(tailKey)
          nearStableValueList(newKey) = subList
        }
      }

      if (nearStableValueList.size == 1 && nearStableValueList.head._2.size == 24 && key != headTail) {
        nearStableValueList(headTail) = nearStableValueList(key)
        nearStableValueList.remove(key)
      }
    }
    nearStableValueList.toList
  }

  def top3NearStable(peak: Boolean, weekMax:Float, minPeriod:String, min:Float, coefficientMap: mutable.LinkedHashMap[String, (Float, Seq[Float])], valueKeys:Seq[Int], coefficientLimit: Float, condition: String, valueLimit:Float, minSize: Int) :Seq[String] = {
    val cn = if (peak) "波峰" else "波谷"
    val row = mutable.ListBuffer[String]()
    // 按小时数排倒序，取前三
    val tmpNearStableValueList = if (peak) nearStablePeak(coefficientMap, valueKeys, coefficientLimit, condition, valueLimit, minSize) else nearStableThrough(coefficientMap, valueKeys, coefficientLimit, condition, valueLimit, minSize)
    val finalNearStableValueList = tmpNearStableValueList.map(kv => {
        val (key, children) = kv
        val childMean = children.sum / children.length
        val coefficient = (children.max - min) / (weekMax - min)
        (key, (childMean, children.max, coefficient, children))
      })

    if (logger.isDebugEnabled) {
      logger.debug(s"${cn}附近大致平稳周期")
      finalNearStableValueList.foreach(kv => {
        val (key, (childMean, childMax, coefficient, children)) = kv
        logger.debug(s"[$key]:[$childMean]:[$childMax]:[$coefficient]:[${children.mkString(",")}]")
      })
    }

    finalNearStableValueList.slice(0, 3).foreach(kv => {
      val (key, (childMean, childMax, coefficient, _)) = kv
      row.append(s"'$key'")
      row.append(childMean.toString)
      row.append(childMax.toString)
      row.append(coefficient.toString)
    })
    1.to(3 - finalNearStableValueList.length).foreach(_ => 1.to(4).foreach(_ => row.append(null)))

    val periodKeys = periodToList(minPeriod)
    val containPeriodNearStableValueList = finalNearStableValueList.filter(kv => {
      val nearStableKeys = periodToList(kv._1)
      periodKeys.count(nearStableKeys.contains(_)) == periodKeys.length
    })

    if (containPeriodNearStableValueList.nonEmpty) {
      logger.debug(s"大致平稳周期-包含${cn}的周期")
      containPeriodNearStableValueList.sortWith((x, y) => x._2._4.size > y._2._4.size).slice(0, 1).foreach(kv => {
        val (key, (childMean, childMax, coefficient, children)) = kv
        logger.debug(s"[$key]:[$childMean]:[$childMax]:[$coefficient]:[${children.mkString(",")}]")

        row.append(s"'$key'")
        row.append(childMean.toString)
        row.append(childMax.toString)
        row.append(coefficient.toString)
      })
    } else {
      1.to(4).foreach(_ => row.append(null))
    }

    row
  }

  def calculateCore(values: Seq[Float], valueKeys: Seq[Int]): String = {
    if (values.length <= 2) {
      ""
    } else {
      val sorted = increaseOrDecrease(values)
      if (sorted) {
        fmt2Bit(valueKeys(valueKeys.length / 2 + 1))
      } else {
        val troughMin = values.min
        val troughMinIndex = values.indexWhere(_ == troughMin)
        var troughCore = ""
        if (troughMinIndex >= values.length / 2) {
          // 最左向最低点移
          var i = 0
          breakable {
            while (i <= troughMinIndex - 2) {
              val subValues = values.slice(i, troughMinIndex + 1)
              val sorted = increaseOrDecrease(subValues)
              if (sorted) {
                // 递增,递减
                val troughCoreIndex = values.indexWhere(_ == subValues(subValues.length / 2))
                logger.info(s"$troughCoreIndex")
                logger.info(s"${valueKeys.mkString(",")}")
                troughCore = fmt2Bit(valueKeys(troughCoreIndex))
                break
              }
              i = i + 1
            }
          }

          // 最低点向最左移
          if (troughCore == "") {
            breakable {
              i = troughMinIndex
              while (i >= 3) {
                val subValues = values.slice(0, i)
                val sorted = increaseOrDecrease(subValues)
                if (sorted) {
                  // 递增,递减
                  val troughCoreIndex = values.indexWhere(_ == subValues(subValues.length / 2))
                  troughCore = fmt2Bit(valueKeys(troughCoreIndex))
                  break
                }
                i = i - 1
              }
            }
          }
        }

        if (troughCore == "") {
          // 最右向最低点移
          var j = values.length
          breakable {
            while (j >= troughMinIndex + 3) {
              val subValues = values.slice(troughMinIndex, j)
              val sorted = increaseOrDecrease(subValues)
              if (sorted) {
                // 递增,递减
                val troughCoreIndex = values.indexWhere(_ == subValues(subValues.length / 2))
                troughCore = fmt2Bit(valueKeys(troughCoreIndex))
                break
              }
              j = j - 1
            }
          }
        }

        if (troughCore == "") {
          // 最低点向最右侈
          var j = troughMinIndex
          breakable {
            while (j <= values.length - 3) {
              val subValues = values.slice(j, values.length)
              val sorted = increaseOrDecrease(subValues)
              if (sorted) {
                // 递增,递减
                val troughCoreIndex = values.indexWhere(_ == subValues(subValues.length / 2))
                troughCore = fmt2Bit(valueKeys(troughCoreIndex))
                break
              }
              j = j + 1
            }
          }
        }

        troughCore
      }
    }
  }

  def periodToList(period: String, head: Int = 0, tail: Int = 23): Seq[Int] = {
    if (period == null || period.isEmpty) {
      List.empty[Int]
    } else {
      val childHead = period.split(":").head.toInt
      val childTail = period.split(":").last.toInt
      if (childHead <= childTail) {
        childHead.to(childTail)
      } else {
        childHead.to(tail) ++ head.to(childTail)
      }
    }
  }

  def isInteger(str: String): Boolean = {
    if (str == null || str.isEmpty)
      false
    else {
      val number = "[0-9]+".r
      try {
        number.findFirstIn(str).get.length == str.length
      } catch {
        case _: Exception => false
      }
    }
  }

  def fmt2Bit(number: Int): String = if (number < 10) s"0$number" else s"$number"

  def dayOfWeek(calendar: Calendar): Int = if (calendar.get(Calendar.DAY_OF_WEEK) == 1) 7 else calendar.get(Calendar.DAY_OF_WEEK) - 1

  def dayOfWeek(date: Date): Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    dayOfWeek(calendar)
  }
}
