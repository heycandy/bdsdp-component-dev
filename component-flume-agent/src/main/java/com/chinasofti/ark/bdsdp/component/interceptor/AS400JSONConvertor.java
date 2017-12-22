package com.chinasofti.ark.bdsdp.component.interceptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400JDBCDriver;

import com.google.common.collect.Maps;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;

import java8.util.stream.Collectors;
import java8.util.stream.StreamSupport;
import scala.tools.cmd.gen.AnyVals;

/**
 * Created by White on 2017/12/18.
 */
public class AS400JSONConvertor implements Interceptor {

  private Logger logger = LoggerFactory.getLogger(AS400JSONConvertor.class);

  private String dbURL = "jdbc:as400://192.168.88.249;naming=sql;errors=full";
  private String dbUser = "HTXA23002";
  private String dbPassword = "HTXA23002";
  private String dbCatalog = null;
  private String schemaPattern = "ASJRNTRNO2";
  private String tableNamePattern = "SSTNJNP";
  private String columnNamePattern = "%";

  private ObjectMapper mapper;
  private ArrayList<Column> columns;

  public AS400JSONConvertor(Context context) {
    this.dbURL = context.getString("dbURL");
    this.dbUser = context.getString("dbUser");
    this.dbPassword = context.getString("dbPassword");
    this.dbCatalog = context.getString("dbCatalog", null);
    this.schemaPattern = context.getString("schemaPattern");
    this.tableNamePattern = context.getString("tableNamePattern");
    this.columnNamePattern = context.getString("columnNamePattern", "%");
  }

  public static void main(String[] args) {
    Context context = new Context();

    Builder builder = new AS400JSONConvertor.Builder();
    builder.configure(context);

    Interceptor interceptor = builder.build();
    interceptor.initialize();

    Event event =
        EventBuilder.withBody(
            "SSTNJNP   0000093078ACNABCD00123333311023333DDNPM10200 BR19BR192017090300000120170903CNABCD666000666999SSVHKD000000000006N000000000006N00000000000",
            Charset.defaultCharset());
    interceptor.intercept(event);

  }

  /**
   * 连接400 获取表结构，列名和字段长度
   *
   * @return List<Column>
   */
  public ArrayList<Column> getInfo() throws SQLException {
    DriverManager.registerDriver(new AS400JDBCDriver());
    Connection connection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
    DatabaseMetaData dbmd = connection.getMetaData();

    //第一个参数为400系统号，一般为空，可以指定
    ResultSet
        columnRS =
        dbmd.getColumns(dbCatalog, schemaPattern, tableNamePattern, columnNamePattern);

    ArrayList<Column> columns = new ArrayList<>();
    while (columnRS.next()) {
      Column column = new Column();
      column.setName(columnRS.getString("COLUMN_NAME"));
      column.setType(columnRS.getString("TYPE_NAME"));
      column.setSize(columnRS.getInt("COLUMN_SIZE"));

      columns.add(column);

    }

    columnRS.close();
    connection.close();

    return columns;
  }

  @Override
  public void initialize() {
    mapper = new ObjectMapper();
    try {
      columns = this.getInfo();
    } catch (Exception e) {
      logger.error("initialize error", e);
    }
  }

  @Override
  public Event intercept(Event event) {
    //把字段长度做成数组，通过长度将数据拆分成｛fieldname : fieldValue｝
    try {
      Map<String, Object> map = Maps.newHashMap();
      byte[] bytes = event.getBody();

      //tableName,rowNum,action
      byte[] range0 = Arrays.copyOfRange(bytes, 0, 10);
      String tableName = new String(range0).trim();
      map.put("tableName", tableName);

      byte[] range10 = Arrays.copyOfRange(bytes, 10, 20);
      String rowNum = new String(range10);
      map.put("rowNum", Long.valueOf(rowNum));

      byte[] range20 = Arrays.copyOfRange(bytes, 20, 21);
      String action = new String(range20);
      map.put("action", action);

      // {fieldName:fieldValue}
      int j = 0;
      Map<String, Object> row = Maps.newHashMap();
      for (int fieldsStart = 21; fieldsStart < bytes.length; ) {
        int filedLength = columns.get(j).getSize();
        byte[] newBytes = Arrays.copyOfRange(bytes, fieldsStart, fieldsStart + filedLength);

        String key = columns.get(j).getName();
        String value = new String(newBytes);

        if (columns.get(j).getType().equalsIgnoreCase("integer")) {
          row.put(key, Long.valueOf(value));
        } else {
          row.put(key, value);
        }

        fieldsStart = fieldsStart + filedLength;

        j++;
      }

      map.put("row", row);

      String jsonString = mapper.writeValueAsString(map);
      event.setBody(jsonString.getBytes());

      return event;
    } catch (Exception e) {
      logger.error(String.format("intercept error: %s", event), e);
    }

    return null;
  }

  @Override
  public List<Event> intercept(List<Event> list) {
    return StreamSupport.stream(list)
        .filter(event -> event != null)
        .map(this::intercept)
        .collect(Collectors.toList());
  }

  @Override
  public void close() {

  }

  public static class Builder implements Interceptor.Builder {

    private Context _context;

    @Override
    public Interceptor build() {
      return new AS400JSONConvertor(_context);
    }

    @Override
    public void configure(Context context) {
      _context = context;
    }
  }

  public class Column {

    private String name;
    private String type;
    private Integer size;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public Integer getSize() {
      return size;
    }

    public void setSize(Integer size) {
      this.size = size;
    }
  }
}
