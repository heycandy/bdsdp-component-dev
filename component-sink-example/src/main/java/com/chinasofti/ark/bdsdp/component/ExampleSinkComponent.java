package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.data.StringData;
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent;

import org.slf4j.Logger;

/**
 * Created by White on 2017/4/27.
 */
public class ExampleSinkComponent extends SinkComponent<StringData> implements Configureable {

  public ExampleSinkComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps componentProps) {

  }

  @Override
  public void apply(StringData data) {
    info(data.getRawData());
  }
}
