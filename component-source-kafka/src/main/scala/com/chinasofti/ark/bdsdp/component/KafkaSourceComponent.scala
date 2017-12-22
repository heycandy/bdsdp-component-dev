package com.chinasofti.ark.bdsdp.component

import com.chinasofti.ark.bdadp.component.ComponentProps
import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, StreamData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, StreamSourceAdapter}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.Logger

class KafkaSourceComponent(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log)
    with StreamSourceAdapter[StreamData] with Configureable {

  var kafkaParams: Map[String, String] = _
  var topicsSet: Set[String] = _

  override def configure(componentProps: ComponentProps): Unit = {
    kafkaParams = Map(
      "metadata.broker.list" -> componentProps.getString("metadata.broker.list", "localhost:9092"),
      "group.id" -> componentProps.getString("group.id", "default")
    )

    topicsSet = componentProps.getString("topics.set", "default").split(",").toSet
  }

  override def stream(
    sparkScenarioOptions: SparkScenarioOptions): StreamData = {
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sparkScenarioOptions.streamingContext(), kafkaParams, topicsSet)
    val lines = messages.map(_._2)
    Builder.build(lines)
  }

  override def call(): StringData = {
    null
  }
}
