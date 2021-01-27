package com.dyzcs.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * Created by Administrator on 2021/1/27.
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 接收外部参数
        val paramTool = ParameterTool.fromArgs(args)
        val host = paramTool.get("host")
        val port = paramTool.getInt("port")

        // 接收一个socket文本流
        val inputDataStream = env.socketTextStream(host, port)
        // 进行转换处理
        val resultDataStream = inputDataStream.flatMap(_.split(" "))
                .filter(_.nonEmpty)
                .map((_,1))
                .keyBy(0)
                .sum(1)

        resultDataStream.print().setParallelism(1)

        // 启动任务执行
        env.execute("stream word count")
    }
}
