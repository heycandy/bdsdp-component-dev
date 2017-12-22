package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;

import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by White on 2017/4/27.
 */
public class FlumeAgentComponent extends RunnableComponent implements Configureable {

  private Map<String, String> properties = new HashMap<>();
  private EmbeddedAgent agent = null;

  public FlumeAgentComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  public static void main(String[] args) {
    FlumeAgentComponent agent = new FlumeAgentComponent("", "", LoggerFactory.getLogger(""));

    ComponentProps componentProps = new ComponentProps();

    componentProps.setProperty("source.port", "44444");
    componentProps.setProperty("sink1.kafka.bootstrap.servers", "ubuntu:9092");
    componentProps.setProperty("dbURL", "jdbc:as400://192.168.88.249;naming=sql;errors=full");
    componentProps.setProperty("dbUser", "HTXA23002");
    componentProps.setProperty("dbPassword", "HTXA23002");
    componentProps.setProperty("dbCatalog", "");
    componentProps.setProperty("schemaPattern", "ASJRNTRNO2");
    componentProps.setProperty("tableNamePattern", "SSTNJNP");
    componentProps.setProperty("columnNamePattern", "");

    agent.configure(componentProps);
    agent.run();
  }

  @Override
  public void configure(ComponentProps componentProps) {
    properties.put("source.type", "netcat");
    properties.put("source.bind", componentProps.getString("source.bind", "localhost"));
    properties.put("source.port", componentProps.getString("source.port", "44444"));
    properties.put("channel.type", componentProps.getString("channel.type", "memory"));
    properties.put("channel.capacity", componentProps.getString("channel.capacity", "100"));
    properties.put("channel.transactionCapacity",
                   componentProps.getString("channel.transactionCapacity", "100"));
    properties.put("sinks", "sink1");
    properties.put("sink1.type", "org.apache.flume.sink.kafka.KafkaSink");
    properties.put("sink1.kafka.bootstrap.servers",
                   componentProps.getString("sink1.kafka.bootstrap.servers", "localhost:9092"));
    properties.put("sink1.kafka.topic", componentProps.getString("sink1.kafka.topic", "default"));
    properties.put("sink1.kafka.producer.acks", "1");
    properties.put(
        "sink1.kafka.flumeBatchSize", componentProps.getString("sink1.kafka.flumeBatchSize", "1"));
    properties.put("processor.type", "failover");
    properties.put("source.interceptors", "i1");
    properties.put("source.interceptors.i1.type",
                   "com.chinasofti.ark.bdsdp.component.interceptor.AS400JSONConvertor$Builder");
    properties.put("source.interceptors.i1.dbURL", componentProps.getString("dbURL"));
    properties.put("source.interceptors.i1.dbUser", componentProps.getString("dbUser"));
    properties.put("source.interceptors.i1.dbPassword", componentProps.getString("dbPassword"));
    properties.put("source.interceptors.i1.dbCatalog", componentProps.getString("dbCatalog"));
    properties
        .put("source.interceptors.i1.schemaPattern", componentProps.getString("schemaPattern"));
    properties.put("source.interceptors.i1.tableNamePattern",
                   componentProps.getString("tableNamePattern"));
    properties.put("source.interceptors.i1.columnNamePattern",
                   componentProps.getString("columnNamePattern"));
  }

  @Override
  public void run() {
    agent = new EmbeddedAgent(super.getName());
    agent.configure(properties);
    agent.start();
  }

  @Override
  public void stop() {
    if (agent != null) {
      info("Flume Stopping...");
      agent.stop();
      info("Flume Stopped.");
    }
    super.stop();
  }
}
