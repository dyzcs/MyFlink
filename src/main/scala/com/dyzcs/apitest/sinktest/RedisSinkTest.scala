package com.dyzcs.apitest.sinktest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Created by Administrator on 2021/1/28.
 */
object RedisSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("src/main/resources/sensor.txt")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        // 定义一个FlinkJedisConfigBase
        val conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build()

        dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

        env.execute("flink sink to redis")
    }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
        // 定义保存数据写入redis的命令，HSET[redis数据结构] 表名[存入数据的表名]
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
    }

    // 将id指定为key
    override def getKeyFromData(data: SensorReading): String = data.id

    // 将温度值指定为value
    override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
