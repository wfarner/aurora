package com.twitter.mesos.executor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.twitter.mesos.Message;
import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduleStatus;
import mesos.ExecutorDriver;
import mesos.Protos.*;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstraction away from the mesos executor driver.
 *
 * @author William Farner
 */
public interface Driver extends Function<Message, Integer> {

  /**
   * Sends an update on the status of a task.
   *
   * @param taskId Task to update the status for.
   * @param status New status of thet task.
   * @return zero if the status was successfully sent (but not necessarily received), or non-zero
   *    if the status could not be sent.
   */
  public int sendStatusUpdate(String taskId, ScheduleStatus status);

  /**
   * Sets the underlying driver.
   *
   * @param driver Real driver.
   * @param executorArgs executor args.
   */
  public void init(ExecutorDriver driver, ExecutorArgs executorArgs);

  public static class DriverImpl implements Driver {

    private static final Logger LOG = Logger.getLogger(DriverImpl.class.getName());

    private final AtomicReference<ExecutorDriver> driverRef = new AtomicReference<ExecutorDriver>();
    private final AtomicReference<ExecutorArgs> executorArgs = new AtomicReference<ExecutorArgs>();

    @Override
    public void init(ExecutorDriver driver, ExecutorArgs executorArgs) {
      LOG.info("Driver assigned " + driver + ", and args " + executorArgs);
      this.driverRef.set(driver);
      this.executorArgs.set(executorArgs);
    }

    /**
     * Convenience wrapper to do work if a driver reference is available.
     *
     * @param work Work to execute with the driver.
     * @return Return code from driver operation.
     */
    private int doWorkWithDriver(Function<ExecutorDriver, Integer> work) {
      ExecutorDriver driver = driverRef.get();

      if (driver == null) {
        LOG.warning("Driver not available, message could not be sent.");
        return -1;
      }

      return work.apply(driver);
    }

    @Override public Integer apply(final Message message) {
      Preconditions.checkNotNull(message);

      int result = doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          FrameworkMessage.Builder messageBuilder = FrameworkMessage.newBuilder()
              .setSlaveId(executorArgs.get().getSlaveId());
          try {
            messageBuilder.setData(
                ByteString.copyFrom(ThriftBinaryCodec.encode(message.getMessage())));
          } catch (CodingException e) {
            LOG.log(Level.SEVERE, "Failed to encode message: " + message.getMessage()
                                  + " intended for slave " + message.getSlaveId());
            return -1;
          }

          LOG.info("Sending message to scheduler.");
          int result = driver.sendFrameworkMessage(messageBuilder.build());
          if (result != 0) {
            LOG.warning(String.format("Attempt to send executor message returned code %d: %s",
                result, message));
          }

          return result;
        }
      });

      if (result != 0) {
        LOG.warning("Attempt to send message failed with code " + result);
      }

      return result;
    }

    @Override public int sendStatusUpdate(final String taskId, final ScheduleStatus status) {
      Preconditions.checkNotNull(status);

      return doWorkWithDriver(new Function<ExecutorDriver, Integer>() {
        @Override public Integer apply(ExecutorDriver driver) {
          LOG.info("Notifying task " + taskId + " in state " + status);
          int result = driver.sendStatusUpdate(
              TaskStatus.newBuilder()
                  .setTaskId(TaskID.newBuilder().setValue(taskId))
                  .setSlaveId(executorArgs.get().getSlaveId())
                  .setState(StateTranslator.get(status))
                  .build());
          if (result != 0) {
            LOG.warning("Attempt to send executor message returned code " + result);
          }
          return result;
        }
      });
    }
  }
}
