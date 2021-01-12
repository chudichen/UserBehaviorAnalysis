package com.chudichen.order

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author chudichen
 * @since 2021-01-12
 */
object TxMacthDetect {

  // 定义侧数据流tag
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取订单事件流
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(_.split(","))
      .map(data => OrderEvent(data(0).trim.toLong, data(1).trim, data(2).trim, data(3).trim.toLong))
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map(_.split(","))
      .map(data => ReceiptEvent(data(0).trim, data(1).trim, data(2).toLong))
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 将两条流连接起来，共同处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch)

    processedStream.getSideOutput(unmatchedPays).print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute("tx match job")
  }

  class TxPayMatch extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到账事件
      val receipt = receiptState.value()
      if (receipt == null) {
        // 如果还没到，那么把pay存入状态，并且注册一个定时器等待
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      } else {
        // 如果已经有receipt，在主流输出匹配信息，清空状态
        out.collect((value, receipt))
        receiptState.clear()
      }
    }

    // 到账事件的处理
    override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if (pay == null) {
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if (payState.value() != null) {
        // receipt没来，输出pay到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }
      if (receiptState.value() != null) {
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }
}

// 定义接收流事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)


