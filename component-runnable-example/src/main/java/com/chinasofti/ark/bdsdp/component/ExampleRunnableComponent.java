package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;

import org.slf4j.Logger;

/**
 * Created by White on 2017/4/27.
 */
public class ExampleRunnableComponent extends RunnableComponent implements Configureable {

  private String name = null;

  public ExampleRunnableComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps componentProps) {

    name = componentProps.getString("param_name", "unname");
  }

  @Override
  public void run() {
    info("Hello, " + name);
  }
}
