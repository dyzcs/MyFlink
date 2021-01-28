package com.dyzcs.apitest.sinktest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties

/**
 * Created by Administrator on 2021/1/28.
 */
object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 从kafka读取数据
        // kafka-console-producer.sh --bootstrap-server s183:9092 -pic sensor
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "s183:9092")
        properties.setProperty("group.id", "consumer-group")
        val stream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))

        // 转换为样例类
        val dataStream = stream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
        })

        // kafka-console-consumer.sh --bootstrap-server s183:9092 --topic sinktest
        dataStream.addSink(new FlinkKafkaProducer[String]("s183:9092", "sinktest", new SimpleStringSchema()))

        env.execute("kafka sink test")
    }
}
