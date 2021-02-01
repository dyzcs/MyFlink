package com.dyzcs.apitest.tabletest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

/**
 * Created by Administrator on 2021/2/1.
 */
object Example {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("src/main/resources/sensor.txt")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        // 1.创建表执行环境
        val tableEnv = StreamTableEnvironment.create(env)
        // 2.基于流创建一张表
        val dataTable = tableEnv.fromDataStream(dataStream)
        // 3.调用table api进行转换
        val resultTable = dataTable.select("id, temperature")
                .filter("id == 'sensor_1'")

        resultTable.toAppendStream[(String, Double)].print("result")

        // 直接用sql实现
        tableEnv.createTemporaryView("dataTable", dataTable)
        val sql = "select id, temperature from dataTable where id = 'sensor_1'"
        val resultSqlTable = tableEnv.sqlQuery(sql)

        resultSqlTable.toAppendStream[(String, Double)].print("result sql")

        env.execute("flink table test")
    }
}
