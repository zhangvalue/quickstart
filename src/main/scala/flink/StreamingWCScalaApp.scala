package flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 使用scala开发Flink的实时处理应用程序
  */
object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tet=env.socketTextStream("localhost",9999)
    //引入隐式转换
    import org.apache.flink.api.scala._
    tet.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      //设置并行度为1
      .setParallelism(1)
    env.execute("StreamingWCScalaApp")
  }
}
