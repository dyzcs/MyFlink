package com.dyzcs.apitest

import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/31.
 */
object StateTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        
    }
}
