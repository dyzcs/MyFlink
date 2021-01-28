package com.dyzcs.apitest.sinktest

import com.dyzcs.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * Created by Administrator on 2021/1/28.
 */
object JDBCSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("src/main/resources/sensor.txt")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
        })

        // 官网上方法失败
        //        dataStream.addSink(JdbcSink.sink[SensorReading](
        //            "insert into sensors(id, timestamp, temperature) values(?, ?, ?)",
        //            (ps: PreparedStatement, s: SensorReading) => {
        //                ps.setString(1, s.id)
        //                ps.setLong(2, s.timestamp)
        //                ps.setDouble(3, s.temperature)
        //            },
        //            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        //                    .withUrl("jdbc:mysql://localhost:3306/mydata")
        //                    .withDriverName("com.mysql.jdbc.Driver")
        //                    .withUsername("root")
        //                    .withPassword("981124")
        //                    .build()
        //        ))

        // 自定义sink
        dataStream.addSink(new MyJdbcSinkFunc)

        env.execute("flink sink jdbc")
    }
}

class MyJdbcSinkFunc extends RichSinkFunction[SensorReading] {
    // 定义连接、预编译语句
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydata?serverTimezone=UTC", "root", "981124")
        insertStmt = conn.prepareStatement("insert into sensors(id, timestamp, temperature) values (?,?,?)")
        updateStmt = conn.prepareStatement("update sensors set timestamp = ?, temperature = ? where id = ?")
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
        // 先执行更新操作，查到就更新
        updateStmt.setLong(1, value.timestamp)
        updateStmt.setDouble(2, value.temperature)
        updateStmt.setString(3, value.id)
        updateStmt.execute()

        // 如果更新没有查到数据，那么就插入
        if (updateStmt.getUpdateCount  == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setLong(2, value.timestamp)
            insertStmt.setDouble(3, value.temperature)
            insertStmt.execute()
        }
    }

    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }
}