package com.dyzcs.apitest.sinktest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/28.
 */
object FileSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据
        val inputStream = env.readTextFile("src/main/resources/sensor.txt")

        // 先转换成样例类类型
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
        // 控制台输出
//        dataStream.print()

//        dataStream.writeAsCsv("src/main/resources/out.csv")
        dataStream.

        env.execute("file sink to file")
    }
}
