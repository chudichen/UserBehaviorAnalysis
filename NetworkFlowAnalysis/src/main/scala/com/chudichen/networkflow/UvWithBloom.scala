package com.chudichen.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * 使用布隆过滤器分析用户行为
 *
 * @author chudichen
 * @since 2021-01-05
 */
object UvWithBloom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new UvCountWithBloom)

    dataStream.print()
    env.execute("uv with bloom job")
  }
}

/**
 * 自定义触发器
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

  /**
   * 每来一条数据，就直接出发窗口操作，并清空所有窗口状态
   */
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  /** 定义redis连接 */
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    val value = jedis.hget("count", storeKey)
    val count = if (value == null) 0L else value.toLong

    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist = jedis.getbit(storeKey, offset)

    if (isExist) {
      out.collect(UvCount(storeKey.toLong, count))
    } else {
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
    }
  }
}

class Bloom(size: Long) extends Serializable {

  /** 位图的总大小，默认16M */
  private val cap = if (size > 0) size else 1 << 27

  /**
   * 定义hash函数
   *
   * @param value 参数
   * @param seed 种子
   * @return hash
   */
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}
