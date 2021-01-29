package com.dyzcs

import com.dyzcs.apitest.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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

        // 继承FilterFunction完成过滤
        val filterStream = dataStream.filter(new MyFilter)
//        filterStream.print("filter")

        // 分组聚合，输出每个传感器当前最小值
        // 根据id分组
        val aggStream = dataStream.keyBy(_.id)
                .minBy("temperature")
//        aggStream.print("agg")

        val resultStream1 = dataStream.keyBy(_.id)
                .reduce((curState, newData) => {
                    SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
                })
//        resultStream1.print("result1")

        val resultStream2 = dataStream.keyBy(_.id)
                .reduce(new MyReduceFunction)
//        resultStream2.print("result2")

        val lowTag = new OutputTag[SensorReading]("low");
        val high = new OutputTag[SensorReading]("high")
        val sideOutStream = dataStream.process(new ProcessFunction[SensorReading, SensorReading] {
            override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
                if (value.temperature <= 10) {
                    ctx.output(lowTag, value)
                } else {
                    ctx.output(high, value)
                }
            }
        })

        sideOutStream.getSideOutput(lowTag).print("low")
        sideOutStream.getSideOutput(high).print("high")

        env.execute("flink transform test")
    }
}

class MyFilter extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean =
        value.id.startsWith("sensor_1")
}

class MyReduceFunction extends ReduceFunction[SensorReading]{
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
        SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
}