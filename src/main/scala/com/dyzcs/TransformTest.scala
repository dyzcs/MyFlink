package com.dyzcs

import com.dyzcs.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/28.
 */
object TransformTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("src/main/resources/sensor.txt")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
    }
}
