package com.chinasofti.ark.bdsdp.component

import com.chinasofti.ark.bdadp.component.ComponentProps
import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.StreamData
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import org.slf4j.Logger

/**
  * Created by White on 2017/11/28.
  */
class HdfsAppendComponent(id: String, name: String, log: Logger)
  extends SinkComponent[StreamData[String]](id, name, log)
    with Configureable {

  var key: String = _

  override def configure(
    componentProps: ComponentProps): Unit = {
    key = componentProps.getString("key", "default")
  }

  override def apply(
    inputT: StreamData[String]): Unit = {
    inputT.getRawData.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        iter.foreach(line => line)


      })
    })

  }
}
