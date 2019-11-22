package com.gsta

import java.sql.{DriverManager, Statement}
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source

object GenerateProfile extends Util with Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val yearDate = args(0)
    val yearTable = args(1)
    val date = args(2)
    val dwprbSchema = Source.fromFile(args(3).split(",").head)
    val dwprbFields = dwprbSchema.getLines().map(_.trim.split("\\s+")).map(columns => (columns(0), columns(1))).toList
    val maxactSchema = Source.fromFile(args(3).split(",").last)
    val maxactFields = maxactSchema.getLines().map(_.trim.split("\\s+")).map(columns => (columns(0), columns(1))).toList

    val categoryTable = args(4)
    val corrTable = args(5)
    val stabilityTable = args(6)

    val targetSchema = Source.fromFile(args(7))
    val insertFields = targetSchema.getLines().map(_.trim.split("\\s+")).map(columns => (columns(0), columns(1))).toList
    val targetTable = args(8)
    val city = if (args.last.startsWith("city")) args.last.split(":").last else ""

    dwprbSchema.close()
    maxactSchema.close()
    // categoryDataFile.close()
    targetSchema.close()

    val middleStart = date.split(",").head
    val middleEnd = date.split(",").last
    val middleStartCal = Calendar.getInstance()
    val middleEndCal = Calendar.getInstance()
    middleStartCal.setTime(dateFmt.parse(middleStart.split(",").head))
    middleEndCal.setTime(dateFmt.parse(middleEnd.split(",").last))
    val weeks = ((middleEndCal.getTimeInMillis - middleStartCal.getTimeInMillis) / (3600 * 24 * 1000) + 1) / 7

    val monday = Calendar.getInstance()
    val sunday = Calendar.getInstance()
    monday.setTime(dateFmt.parse(yearDate.split(",").head))
    sunday.setTime(dateFmt.parse(yearDate.split(",").last))
    val weekDateMap = ((1, (dateFmt.format(monday.getTime), dateFmt.format(sunday.getTime))) +: 2.to(52).map(i => {
      monday.add(Calendar.DATE, 7)
      sunday.add(Calendar.DATE, 7)
      (i, (dateFmt.format(monday.getTime), dateFmt.format(sunday.getTime)))
    })).toMap

    val weekIndex = weekDateMap.filter(kv => kv._2._1 == middleStart || kv._2._2 == middleEnd).keys.toList.sorted
    val weekIndexHead = weekIndex.head
    val weekIndexTail = weekIndex.last

    // mysql
    Class.forName(mysqlDriver)
    val mysqlConnect = DriverManager.getConnection(mysqlJdbcUrl)
    val mysqlStmt = mysqlConnect.createStatement()
    mysqlConnect.setAutoCommit(false)

    val dwprbYearMap = loadDataFromMysql(mysqlStmt, yearTable.split(",").head, dwprbFields)
    val maxactYearMap = loadDataFromMysql(mysqlStmt, yearTable.split(",").last, maxactFields)
    val categoryMap = loadCategoryDataFromMysql(mysqlStmt, categoryTable)
    val corrMap = loadCorrFromMysql(mysqlStmt, corrTable)
    val stabilityMap = loadStabilityFromMysql(mysqlStmt, stabilityTable)

    // hive
    val sc = new SparkContext()
    val hive = new HiveContext(sc)
    hive.sql("use noce")

    val dwprb24ValueColumnList = 0.to(23).map(hour => {
      s"dwprbH${fmt2Bit(hour)}_fill"
    })
    val dwprbSql = s"select base_statn_id, cell_id, day, day_of_week, dwprbmax, dwprbmean, ${dwprb24ValueColumnList.mkString(",")}, low1_dwprb_period, low2_dwprb_period, low3_dwprb_period, low4_dwprb_period, low5_dwprb_period, low6_dwprb_period, low7_dwprb_period, trough, peak, near_stable_1st, near_stable_contain_trough from WID_DWPRB_DAY_WEEKMAXDAYMIN where day >= $middleStart and day <= $middleEnd order by day"
    val maxactSql = s"select base_statn_id, cell_id, day, day_of_week, maxactmax, zero_actuser_period, less_actuser_period from WID_MAXACT_DAY_WEEKMAXDAYMIN where day >= $middleStart and day <= $middleEnd order by day"

    val dwprbData = hive.sql(dwprbSql).collect().groupBy(row => (row.getInt(0).toString, row.getInt(1).toString))
    val maxactData = hive.sql(maxactSql).collect().groupBy(row => (row.getInt(0).toString, row.getInt(1).toString))
    val sectorList = generateSectorMap(hive, "20190501", city)

    val sqlClause = mutable.ListBuffer[String]()
    sectorList.foreach(kv => {
      val key = kv._1
      val sectorRow = kv._2
      val is_indoor = sectorRow.getString(5)
      if (maxactYearMap.contains(key) || dwprbYearMap.contains(key) || maxactData.contains(key) || dwprbData.contains(key)) {
        val row = generateSectorInfo(sectorRow)

        row.append(middleStart)
        row.append(middleEnd)

        if (maxactYearMap.contains(key)) {
          val maxactRecord = maxactYearMap(key)
          val maxactValues = 1.to(52).map(i => {
            val week = if (i < 10) s"0$i" else s"$i"
            maxactRecord(s"maxact_max_w$week").toFloat
          })
          row.append(maxactValues.count(_ == 0).toString)
          row.append(if ("室内" == is_indoor) maxactValues.count(x => 0 <= x && x < 2).toString else maxactValues.count(x => 0 <= x && x < 3).toString)
          row.append(maxactValues.count(x => 60 < x).toString)
        } else {
          16.to(18).foreach(_ => row.append(null))
        }

        if (dwprbYearMap.contains(key)) {
          val dwprbRecord = dwprbYearMap(key)
          val dwprbValues = 1.to(52).map(i => {
            val week = if (i < 10) s"0$i" else s"$i"
            dwprbRecord(s"dwprb_max_w$week").toFloat
          })

          val peak = dwprbRecord("peak")
          val dwprb_max = dwprbRecord("dwprb_max").toFloat
          val peak_near_stable_1st = dwprbRecord("peak_near_stable_1st")
          val trough = dwprbRecord("trough")
          val trough_max = dwprbRecord("trough_max").toFloat
          val near_stable_contain_trough = dwprbRecord("near_stable_contain_trough")
          val near_stable_contain_trough_max = dwprbRecord("near_stable_contain_trough_max").toFloat
          val peak_near_stable_1st_mean = dwprbRecord("peak_near_stable_1st_mean").toFloat
          val near_stable_1st_max = dwprbRecord("near_stable_1st_max").toFloat
          val near_stable_1st = dwprbRecord("near_stable_1st")
          val rank = dwprbRecord("rank").toInt
          row.append(dwprbValues.count(x => 0 < x && x <= 0.05).toString)
          row.append(dwprbValues.count(x => 0 < x && x <= 0.1).toString)
          row.append(dwprbValues.count(x => 0.6 < x).toString)

          var tideType = -1
          if (peak != null && !peak.isEmpty && peak_near_stable_1st != null
            && trough != null && !trough.isEmpty && near_stable_contain_trough != null && near_stable_1st != null
            && dwprb_max > 0.1
            && peak_near_stable_1st.split(":").last.toInt - peak_near_stable_1st.split(":").head.toInt > 5
            && ((math.abs(trough.split(":").last.toInt - trough.split(":").head.toInt) > 0 && trough_max < 0.15)
            || (math.abs(near_stable_contain_trough.split(":").last.toInt - near_stable_contain_trough.split(":").head.toInt) > 0 && near_stable_contain_trough_max < 0.15))
            && peak_near_stable_1st_mean / near_stable_1st_max > 4
            && near_stable_1st.split(":").last.toInt - near_stable_1st.split(":").head.toInt > 1
            && rank < 13) {
            val year_tide_list = 1.to(12).map(i => dwprbRecord(s"tide${if (i < 10) s"0$i" else s"$i"}"))
            row.append("1")
            row.appendAll(year_tide_list.map(x => s"'$x'"))

            year_tide_list.filter(x => x != null && !x.isEmpty).foreach(tide => {
              val tideHead = tide.split(",").head
              /*val dateRange = tideHead.split("\\[").last.replace("]", "")
              val start = dateRange.split("-").head
              val end = dateRange.split("-").last*/
              val index = tideHead.split("\\[").head
              val indexStart = index.split("-").head.toInt
              val indexEnd = index.split("-").last.toInt
              if ((indexStart > indexEnd && indexStart <= weekIndexHead) || (indexStart <= weekIndexHead && weekIndexTail <= indexEnd)) {
                tideType = tide.split(",")(1).toInt
              }
            })
          } else {
            row.append("0")
            1.to(12).foreach(_ => row.append(null))
          }

          if (tideType == -1)
            row.append(null)
          else
            row.append(tideType.toString)
        } else {
          19.to(35).foreach(_ => row.append(null))
        }

        val categoryData = if (categoryMap.contains(key)) categoryMap(key) else List.empty[String]
        if (categoryData.nonEmpty) {
          val merge_after_is_pure = categoryData.head
          val sihouette_score = categoryData.last
          val categoryList = categoryData.slice(1, categoryData.length - 1)
          row.append(merge_after_is_pure)
          row.append(sihouette_score)
          categoryList.foreach(category => {
            if (category.nonEmpty)
              row.append(s"'${"'\\d':\\s*\\d".r.findAllIn(category).map(_.substring(1, 2)).toList.sorted.mkString("")}'")
            else
              row.append(null)
          })
        } else {
          36.to(41).foreach(_ => row.append(null))
        }

        if (maxactData.contains(key)) {
          // 中近期特征
          val maxactRows = maxactData(key)
          val maxactmaxValues = maxactRows.map(maxactRow => {
            maxactRow.getFloat(4)
          })
          val zero_actuser_day = maxactmaxValues.count(_ == 0)
          val less_actuser_day = maxactmaxValues.count(x => if ("室内" == is_indoor) 0 <= x && x < 2 else 0 <= x && x < 3)
          val more_actuser_day = maxactmaxValues.count(_ > 60)
          row.append(zero_actuser_day.toString)
          row.append(less_actuser_day.toString)
          row.append(more_actuser_day.toString)

          if (categoryData.nonEmpty) {
            val merge_after_is_pure = categoryData.head.toInt
            val categoryList = categoryData.slice(1, categoryData.length - 1)
            val maxactIndexRange = 5.to(6)
            if (0 == merge_after_is_pure) {
              // 只一类
              // zero_actuser
              // less_actuser
              maxactIndexRange.foreach(idx => {
                val periods = maxactRows.filter(datarow => datarow.getString(idx) != null && datarow.getString(idx).nonEmpty).flatMap(_.getString(idx).split(","))
                val hourList = calculateHourList(periods)
                if (hourList.nonEmpty) {
                  row.append(s"'${gather(hourList)}'")
                  2.to(4).foreach(_ => row.append(null))
                } else {
                  1.to(4).foreach(_ => row.append(null))
                }
              })
            } else {
              // 分类
              val dayListSeq = categoryList.map(category => {
                if (category.nonEmpty) {
                  "'\\d':\\s*\\d".r.findAllIn(category).map(_.substring(1, 2).toInt).toList
                } else {
                  List[Int]()
                }
              })
              // zero_actuser
              // less_actuser
              maxactIndexRange.foreach(idx => {
                dayListSeq.foreach(dayList => {
                  if (dayList.nonEmpty) {
                    val periods = dayList.flatMap(day => {
                      maxactRows.filter(datarow => datarow.getInt(3) == day && datarow.getString(idx) != null && datarow.getString(idx).nonEmpty).flatMap(_.getString(idx).split(","))
                    })
                    val hourList = calculateHourList(periods)
                    if (hourList.nonEmpty) {
                      row.append(s"'${gather(hourList)}'")
                    } else {
                      row.append(null)
                    }
                  } else {
                    row.append(null)
                  }
                })
              })
            }
          } else {
            45.to(52).foreach(_ => row.append(null))
          }
        } else {
          42.to(52).foreach(_ => row.append(null))
        }

        if (dwprbData.contains(key)) {
          val dwprbRows = dwprbData(key)
          val dwprbmaxValues = dwprbRows.map(dwprbRow => {
            dwprbRow.getFloat(4)
          })
          row.append(dwprbmaxValues.count(_ <= 0.05f).toString)
          row.append(dwprbmaxValues.count(_ <= 0.1f).toString)
          row.append(dwprbmaxValues.count(_ >= 0.6f).toString)

          if (categoryData.nonEmpty) {
            val merge_after_is_pure = categoryData.head.toInt
            val categoryList = categoryData.slice(1, categoryData.length - 1)
            val pwdprbIndexRange = 30.to(40)
            // 凡是取交集的情况，多周中有任何一周无数据，则置为空，多周数据齐全但是无交集则为0 ，注意区别于交集为0点的0:0
            if (0 == merge_after_is_pure) {
              // 只一类
              // low1~low7
              // through_dwprb
              // peak_dwprb
              // near_stable_1st_dwprb
              // near_stable_contain_trough_dwprb
              var near_stable_1st_pub = ""
              pwdprbIndexRange.foreach(idx => {
                val periods = dwprbRows.filter(datarow => datarow.getString(idx) != null && datarow.getString(idx).nonEmpty).flatMap(_.getString(idx).split(","))
                val hourList = calculateHourList(periods)
                if (hourList.nonEmpty) {
                  val pubHour = gather(hourList)
                  if (idx == 39) {
                    near_stable_1st_pub = pubHour
                  }
                  row.append(s"'$pubHour'")
                  2.to(4).foreach(_ => row.append(null))
                } else {
                  1.to(4).foreach(_ => row.append(null))
                }
              })
              // mean
              row.append((dwprbRows.map(_.getFloat(5)).sum / (weeks * 7)).toString)
              2.to(4).foreach(_ => row.append(null))
              // 1st_mean
              // near_stable_1st_pub 有可能为0
              if (near_stable_1st_pub.length > 1) {
                val hourList = periodToList(near_stable_1st_pub)
                row.append((dwprbRows.flatMap(datarow => {
                  hourList.map(hour => datarow.getFloat(hour + 6))
                }).sum / (weeks * 7 * hourList.length)).toString)
              } else {
                row.append(null)
              }
              2.to(4).foreach(_ => row.append(null))
              // week_pattern
              row.append("'1111111'")
            } else {
              // 分类
              val dayListSeq = categoryList.map(category => {
                if (category.nonEmpty) {
                  "'\\d':\\s*\\d".r.findAllIn(category).map(_.substring(1, 2).toInt).toList
                } else {
                  List[Int]()
                }
              })
              // low1~low7
              // through_dwprb
              // peak_dwprb
              // near_stable_1st_dwprb
              // near_stable_contain_trough_dwprb
              val near_stable_1st_pubList = mutable.ListBuffer[String]()
              pwdprbIndexRange.foreach(idx => {
                dayListSeq.foreach(dayList => {
                  if (dayList.nonEmpty) {
                    val periods = dayList.flatMap(day => {
                      dwprbRows.filter(datarow => datarow.getInt(3) == day && datarow.getString(idx) != null && datarow.getString(idx).nonEmpty).flatMap(_.getString(idx).split(","))
                    })
                    val hourList = calculateHourList(periods)
                    if (hourList.nonEmpty) {
                      val pubHour = gather(hourList)
                      if (idx == 39) {
                        near_stable_1st_pubList.append(pubHour)
                      }
                      row.append(s"'$pubHour'")
                    } else {
                      if (idx == 39) {
                        near_stable_1st_pubList.append("")
                      }
                      row.append(null)
                    }
                  } else {
                    if (idx == 39) {
                      near_stable_1st_pubList.append("")
                    }
                    row.append(null)
                  }
                })
              })
              // mean
              val dayMeanMap = mutable.LinkedHashMap[Seq[Int], Float]()
              dayListSeq.foreach(dayList => {
                if (dayList.nonEmpty) {
                  val mean = dayList.flatMap(day => {
                    dwprbRows.filter(_.getInt(3) == day).map(_.getFloat(5))
                  }).sum / (weeks * dayList.length)
                  dayMeanMap(dayList) = mean
                  row.append(mean.toString)
                } else {
                  row.append(null)
                }
              })
              val sortedDayMean = dayMeanMap.toList.sortBy(_._2)
              val dayPatternMap = mutable.HashMap[Int, Int]()
              sortedDayMean.indices.foreach(i => {
                val kv = sortedDayMean(i)
                kv._1.foreach(day => dayPatternMap(day) = i + 1)
              })
              // 1st_mean
              near_stable_1st_pubList.indices.foreach(i => {
                val near_stable_1st_pub = near_stable_1st_pubList(i)
                if (near_stable_1st_pub.length > 1) {
                  val hourList = periodToList(near_stable_1st_pub)
                  val dayList = dayListSeq(i)
                  row.append((dayList.flatMap(day => {
                    dwprbRows.filter(_.getInt(3) == day).flatMap(datarow => {
                      hourList.map(hour => datarow.getFloat(hour + 6))
                    })
                  }).sum / (weeks * dayList.length * hourList.length)).toString)
                } else {
                  row.append(null)
                }
              })
              // week_pattern
              row.append(dayPatternMap.toList.sortBy(_._1).map(_._2).mkString(""))
            }
          } else {
            56.to(108).foreach(_ => row.append(null))
          }
        } else {
          53.to(108).foreach(_ => row.append(null))
        }

        if (corrMap.contains(key)) {
          row.append(corrMap(key).toString)
        } else {
          row.append(null)
        }

        if (stabilityMap.contains(key)) {
          val maxMinList = stabilityMap(key)
          maxMinList.foreach(maxMin => {
            if (maxMin == 0)
              row.append(null)
            else
              row.append(maxMin.toString)
          })
        } else {
          110.to(113).foreach(_ => row.append(null))
        }

        // logger.info(row.mkString("|"))
        sqlClause.append(s"(${row.mkString(",").replaceAll("'null'", "null")})")
      }
    })

    mysqlStmt.execute(s"delete from `$targetTable`")
    mysqlConnect.commit()

    val pageSize = 2000
    val insertColumns = insertFields.map(field => field._1)
    val pageCount = sqlClause.length / pageSize
    0.to(pageCount).foreach(page => {
      val sql = s"insert into `$targetTable` (${insertColumns.mkString(",")}) values ${sqlClause.slice(page * pageSize, (page + 1) * pageSize).mkString(",")}"
      try {
        mysqlStmt.execute(sql)
        mysqlConnect.commit()
      } catch {
        case _: Exception => logger.error(sql)
      }
    })

    mysqlStmt.close()
    mysqlConnect.close()
    sc.stop()
  }

  def calculateHourList(periods: Seq[String]): Seq[Int] = {
    if (periods.nonEmpty) {
      val delta = periods.length * 0.8f
      periods.flatMap(period => {
        periodToList(period)
      }).groupBy(x => x).mapValues(_.length).filter(_._2 > delta).toList.sortBy(_._1).map(_._1)
    } else {
      List()
    }
  }

  def gather(hourList: Seq[Int]): String = {
    if (hourList.isEmpty) {
      "0"
    } else {
      if (hourList.size == 2) {
        if (hourList.last - hourList.head == 1) {
          s"${fmt2Bit(hourList.head)}:${fmt2Bit(hourList.last)}"
        } else {
          s"${fmt2Bit(hourList.head)}:${fmt2Bit(hourList.head)}"
        }
      } else {
        val flagList = 0.to(23).map(i => {
          if (hourList.contains(i))
            1
          else
            0
        })
        // 选连续最长的
        val continueMap = gatherContinueFlags(0.to(23), flagList).filter(_._2 == 1).map(kv => {
          val head = kv._1.split(":").head.toInt
          val tail = kv._1.split(":").last.toInt
          (kv._1, tail - head + 1)
        })
        if (continueMap.size > 1) {
          val lastKey = continueMap.last._1
          if (lastKey.split(":").last.toInt == 23) {
            val headKey = continueMap.head._1
            continueMap(s"${lastKey.split(":").head}:${headKey.split(":").last}") = continueMap(headKey) + continueMap(lastKey)
            continueMap.remove(headKey)
            continueMap.remove(lastKey)
          }

          continueMap.toList.maxBy(_._2)._1
        } else {
          continueMap.head._1
        }
      }
    }
  }

  def loadDataFromMysql(statement: Statement, table: String, fields: Seq[(String, String)]): mutable.HashMap[(String, String), mutable.Map[String, String]] = {
    val values = mutable.HashMap[(String, String), mutable.Map[String, String]]()
    val rs = statement.executeQuery(s"SELECT * FROM `$table`")
    while (rs.next()) {
      val row = mutable.HashMap[String, String]()
      fields.indices.foreach(i => {
        val columnName = fields(i)._1.replaceAll("`", "")
        val columnType = fields(i)._2
        if (columnType.startsWith("int") || columnType.startsWith("TINYINT")) {
          row(columnName) = rs.getInt(i + 1).toString
        } else if (columnType.startsWith("float")) {
          row(columnName) = rs.getFloat(i + 1).toString
        } else {
          row(columnName) = rs.getString(i + 1)
        }
      })
      values((rs.getInt(2).toString, rs.getInt(4).toString)) = row
    }
    rs.close()
    values
  }

  def loadCategoryDataFromMysql(statement: Statement, table: String): mutable.HashMap[(String, String), Seq[String]] = {
    val values = mutable.HashMap[(String, String), Seq[String]]()
    val rs = statement.executeQuery(s"SELECT `base_statn_id`, `cell_id`, `merge_after_is_pure`, `c9`, `c10`, `c11`, `c12`, `silhouette_score` FROM `$table`")
    while (rs.next()) {
      values((rs.getInt(1).toString, rs.getInt(2).toString)) =
        List(
          rs.getInt(3).toString,
          rs.getString(4),
          rs.getString(5),
          rs.getString(6),
          rs.getString(7),
          rs.getFloat(8).toString
        )
    }
    rs.close()
    values
  }

  def loadCorrFromMysql(statement: Statement, table: String): mutable.HashMap[(String, String), Float] = {
    val values = mutable.HashMap[(String, String), Float]()
    val rs = statement.executeQuery(s"SELECT `baseid`, `cellid`, `corr_min` FROM `$table` where `corr_min` is not null")
    while (rs.next()) {
      values((rs.getInt(1).toString, rs.getInt(2).toString)) = rs.getFloat(3)
    }
    rs.close()
    values
  }

  def loadStabilityFromMysql(statement: Statement, table: String): mutable.HashMap[(String, String), Seq[Float]] = {
    val values = mutable.HashMap[(String, String), Seq[Float]]()
    val rs = statement.executeQuery(s"SELECT `base_statn_id`, `cell_id`, `week1to4_class01_max_min`, `week1to4_class02_max_min`, `week1to4_class03_max_min`, `week1to4_class04_max_min` FROM `$table`")
    while (rs.next()) {
      values((rs.getInt(1).toString, rs.getInt(2).toString)) =
        List(
          rs.getFloat(3),
          rs.getFloat(4),
          rs.getFloat(5),
          rs.getFloat(6)
        )
    }
    rs.close()
    values
  }
}
