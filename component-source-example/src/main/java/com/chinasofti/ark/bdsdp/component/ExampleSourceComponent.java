package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.data.Builder;
import com.chinasofti.ark.bdadp.component.api.data.StringData;
import com.chinasofti.ark.bdadp.component.api.source.SourceComponent;

import org.slf4j.Logger;

/**
 * Created by White on 2017/4/27.
 */
public class ExampleSourceComponent extends SourceComponent<StringData> implements Configureable {

  private String name = null;

  public ExampleSourceComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps componentProps) {
    name = componentProps.getString("param_name", "unnamed");
  }

  @Override
  public StringData call() {
    return Builder.build("Hello, " + name);
  }
}
