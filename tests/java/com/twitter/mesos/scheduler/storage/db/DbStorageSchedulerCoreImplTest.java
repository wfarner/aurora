package com.twitter.mesos.scheduler.storage.db;

import com.twitter.mesos.scheduler.BaseSchedulerCoreImplTest;
import com.twitter.mesos.scheduler.storage.Storage;

import java.sql.SQLException;

/**
 * @author John Sirois
 */
public class DbStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  @Override
  protected Storage createStorage() throws SQLException {
    return DbStorageTestUtil.setupStorage(this);
  }

  @Override
  protected void expectRestore() throws Exception {
    // noop
  }

  @Override
  protected void expectPersists(int count) throws Exception {
    // noop
  }
}
