package com.dyzcs.apitest

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/31.
 */
object StateTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("localhost", 9999)

        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        env.execute("flink state test")
    }
}

class MyRichMapper extends RichMapFunction[SensorReading, String] {
    // 第一种实现方法: lazy加载
    //    lazy val valueState: ValueState[Double] =
    //        getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

    lazy val listState: ListState[Int] =
        getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))

    lazy val mapState: MapState[Double, Int] =
        getRuntimeContext.getMapState(
            new MapStateDescriptor[Double, Int]("mapState", classOf[Double], classOf[Int]))

    // 第二种实现方法: 将变量声明在外面
    var valueState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
        valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
    }

    override def map(value: SensorReading): String = {
        // 状态的读写
        val myV = valueState.value()
        valueState.update(value.temperature)
        value.id
    }
}
