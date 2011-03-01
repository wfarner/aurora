package com.twitter.mesos.executor;

import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;

import java.io.File;

/**
 * @author William Farner
 */
public interface Task {

  public String getId();

  public void stage() throws TaskRunException;

  public void run() throws TaskRunException;

  public ScheduleStatus blockUntilTerminated();

  public boolean isRunning();

  public void terminate(ScheduleStatus terminalState);

  public File getSandboxDir();

  public AssignedTask getAssignedTask();

  public ScheduleStatus getScheduleStatus();

  public ResourceConsumption getResourceConsumption();

  public static class TaskRunException extends Exception {
    public TaskRunException(String msg, Throwable t) {
      super(msg, t);
    }

    public TaskRunException(String msg) {
      super(msg);
    }
  }
}
