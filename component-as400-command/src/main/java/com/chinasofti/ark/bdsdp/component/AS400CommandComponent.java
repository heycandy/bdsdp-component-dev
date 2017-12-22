package com.chinasofti.ark.bdsdp.component;

import com.chinasofti.ark.bdadp.component.ComponentProps;
import com.chinasofti.ark.bdadp.component.api.Configureable;
import com.chinasofti.ark.bdadp.component.api.RunnableComponent;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.ProgramCall;
import com.ibm.as400.access.CommandCall;

import org.slf4j.Logger;

/**
 * Created by White on 2017/4/27.
 */
public class AS400CommandComponent extends RunnableComponent implements Configureable {

  private String systemName = null;
  private String userName = null;
  private String password = null;
  private String command = null;

  public AS400CommandComponent(String id, String name, Logger log) {
    super(id, name, log);
  }

  @Override
  public void configure(ComponentProps componentProps) {
    command = componentProps.getString("command", "unname");
    systemName = componentProps.getString("systemName", "unname");
    userName = componentProps.getString("userName", "unname");
    password = componentProps.getString("password", "unname");
  }

  @Override
  public void run() {
    String[] commands = command.split("\n");
    AS400 as400 = new AS400(systemName, userName, password);
    try {
      CommandCall cc = new CommandCall(as400);
      if (commands.length > 0) {
        for (String command : commands) {
          cc.run(command);
        }
      }
      as400.disconnectAllServices();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
