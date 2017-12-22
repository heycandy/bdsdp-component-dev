package com.chinasofti.x.bdsdp.component

import com.chinasofti.ark.bdadp.component.api.data.{Builder, StreamData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import org.slf4j.Logger

/**
  * Created by White on 2017/11/28.
  */
class KafkaComponent(id: String, name: String, log: Logger)
  extends SourceComponent[String](id, name, log)
    with SparkSourceAdapter[StreamData[String]] {


  override def spark(
    sparkScenarioOptions: SparkScenarioOptions): StreamData[String] = {


  }

  override def call(): String = {


  }
}
