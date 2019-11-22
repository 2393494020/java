package com.gsta

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable

object GenerateWeek extends Util with Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val hiveTable = args(0).split(":").head
    val columnPrefix = args(0).split(":").last
    val savePath = args(1)
    val directList = savePath.split("/")
    val partition = directList.last
    val monday = partition.split("-").head
    val sunday = partition.split("-").last

    if (dayOfWeek(dateFmt.parse(monday)) != 1 && dayOfWeek(dateFmt.parse(sunday)) != 7) {
      return
    }

    val sc = new SparkContext()
    val hive = new HiveContext(sc)
    hive.sql("use noce")

    val sql = s"select base_statn_id, base_statn_name, cell_id, cell_name, bs_vendor, is_indoor, band, system_type_or_standard, region, attribute, nb_flag, band_width, city_id, ${columnPrefix}min, ${columnPrefix}max, ${columnPrefix}idxmax, ${columnPrefix}idxmin, day_of_week from $hiveTable where day >= $monday and day <= $sunday"
    val data = hive.sql(sql).map(row => ((row.getInt(0), row.getInt(2)), row)).groupByKey().collect()
    val rows = mutable.ListBuffer[String]()
    data.foreach(kv => {
      val row = generateSectorInfo(kv._2.head)
      val values = kv._2.flatMap(dataRow => List(dataRow.getFloat(13), dataRow.getFloat(14)))
      val subDataMap = kv._2.map(dataRow => (dataRow.getInt(17), List(dataRow.getFloat(13).toString, dataRow.getFloat(14).toString, dataRow.getString(15), dataRow.getString(16)))).toMap
      row.append(values.min.toString)
      row.append(values.max.toString)
      1.to(7).foreach(day => {
        if (subDataMap.contains(day)) {
          row.appendAll(subDataMap(day))
        } else {
          row.appendAll(List("", "", "", ""))
        }
      })
      rows.append(row.mkString("|").replaceAll("'", "").replaceAll("null", ""))
    })

    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    sc.parallelize(rows).repartition(1).saveAsTextFile(savePath)

    hive.sql("set hive.exec.dynamic.partition=true")
    hive.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hive.sql(s"alter table ${directList(directList.length - 2)} add IF NOT EXISTS partition(begin_end='$partition') location '$partition/'")

    sc.stop()
  }
}
