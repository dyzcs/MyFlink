package com.dyzcs.apitest

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

/**
 * Created by Administrator on 2021/1/30.
 */
object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // flink 1.12之后默认是这个
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 生成watermark间隔
        env.getConfig.setAutoWatermarkInterval(50)

        val inputStream = env.socketTextStream("localhost", 9999)
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })
                //        .assignTimestampsAndWatermarks(
                //            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //                    .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
                //            override def extractTimestamp(element: SensorReading, recordTimestamp: Long): Long = element.timestamp * 1000L
                //        }))
                // .assignAscendingTimestamps(_.timestamp * 1000L)   // 升序数据提取时间戳
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
                    override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
                })


        // 每15秒统计一次，窗口内各传感器温度的最小值，以及最新的时间戳
        val resultStream = dataStream.keyBy(_.id)
                // .timeWindow(Time.seconds(15)) // 滚动时间窗口简写[已弃用]
                // .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5)))  // 滑动时间窗口
                // .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口
                // .countWindow(10)    // 滚动计数窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15))) // 滚动时间窗口
                // .reduce((curRes, newRes) => SensorReading(curRes.id, curRes.timestamp.max(newRes.timestamp), curRes.temperature.min(newRes.temperature)))
                .reduce(new MyReducer)
        //                .minBy(2)

        resultStream.print("result")

        env.execute("window test")
    }
}

class MyReducer extends ReduceFunction[SensorReading] {
    override def reduce(t1: SensorReading, t2: SensorReading): SensorReading = {
        SensorReading(t1.id, t1.timestamp.max(t2.timestamp), t1.temperature.min(t2.temperature))
    }
}
