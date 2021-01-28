package com.dyzcs.apitest.sinktest

import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/28.
 */
object FileSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 读取数据
        val inputData = env.readTextFile("src/main/resources/sensor.txt")

        // 先转换成样例类类型
    }
}
