package com.dyzcs.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Created by Administrator on 2021/1/30.
 */
object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("localhost", 9999)
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        // 每15秒统计一次，窗口内各传感器温度的最小值，以及最新的时间戳
        val resultStream = dataStream.keyBy(_.id)
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))  // 滚动时间窗口
//                .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5)))  // 滑动时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口
//                .countWindow(10)    // 滚动计数窗口
    }
}
