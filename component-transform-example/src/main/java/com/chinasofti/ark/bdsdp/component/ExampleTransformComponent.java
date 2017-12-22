package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.api.data.Builder;
import com.chinasofti.ark.bdadp.component.api.data.StringData;
import com.chinasofti.ark.bdadp.component.api.transforms.TransformableComponent;

import org.slf4j.Logger;

/**
 * Created by White on 2017/4/27.
 */
public class ExampleTransformComponent extends TransformableComponent<StringData, StringData> {

  public ExampleTransformComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public StringData apply(StringData stringData) {
    return Builder.build(stringData.getRawData().replace("Hello", "Hi"));
  }
}
