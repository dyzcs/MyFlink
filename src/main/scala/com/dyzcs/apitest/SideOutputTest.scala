package com.dyzcs.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Created by Administrator on 2021/1/29.
 */
object SideOutputTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("localhost", 9999)

        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        val lowTag = new OutputTag[SensorReading]("low")
        val highTag = new OutputTag[SensorReading]("high")

        val sideOutputStream1 = dataStream.process(new ProcessFunction[SensorReading, SensorReading] {
            override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
                if (value.temperature <= 9D) {
                    ctx.output(lowTag, value)
                } else {
                    ctx.output(highTag, value)
                }
            }
        })

        val lowStream1 = sideOutputStream1.getSideOutput(lowTag)
//        lowStream1.print("low")

        val highStream1 = sideOutputStream1.getSideOutput(highTag)
//        highStream1.print("high")

        val lowStringTag = new OutputTag[String]("low id")
        val highStringTag = new OutputTag[String]("high id")
        val sideOutputStream2 = dataStream.process(new MySideOut(9D, lowStringTag, highStringTag))
        sideOutputStream2.getSideOutput(lowStringTag).print("low id")
        sideOutputStream2.getSideOutput(highStringTag).print("high id")

        env.execute("sideOutput test")
    }
}

// arr:OutputTag[String]* 可变长度参数
class MySideOut(flag: Double, arr:OutputTag[String]*) extends ProcessFunction[SensorReading, String] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, String]#Context, out: Collector[String]): Unit = {
        if (value.temperature <= flag) {
            ctx.output(arr(0), value.id)
        } else {
            ctx.output(arr(1), value.id)
        }
    }
}