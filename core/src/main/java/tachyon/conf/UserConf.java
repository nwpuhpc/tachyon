package tachyon.conf;

import tachyon.Constants;

public class UserConf extends Utils {
  private static UserConf USER_CONF = null;

  /**
   * This is for unit test only. DO NOT use it for other purpose.
   */
  public static synchronized void clear() {
    USER_CONF = null;
  }

  public static synchronized UserConf get() {
    if (USER_CONF == null) {
      USER_CONF = new UserConf();
    }

    return USER_CONF;
  }

  public final int FAILED_SPACE_REQUEST_LIMITS;
  public final long QUOTA_UNIT_BYTES;
  public final int FILE_BUFFER_BYTES;
  public final long HEARTBEAT_INTERVAL_MS;
  public final long MASTER_CLIENT_TIMEOUT_MS;
  public final long DEFAULT_BLOCK_SIZE_BYTE;
  public final int REMOTE_READ_BUFFER_SIZE_BYTE;
  public final boolean USE_RDMA;

  private UserConf() {
    FAILED_SPACE_REQUEST_LIMITS = getIntProperty("tachyon.user.failed.space.request.limits", 3);
    QUOTA_UNIT_BYTES = getLongProperty("tachyon.user.quota.unit.bytes", 8 * Constants.MB);
    FILE_BUFFER_BYTES = getIntProperty("tachyon.user.file.buffer.bytes", Constants.MB);
    HEARTBEAT_INTERVAL_MS =
        getLongProperty("tachyon.user.heartbeat.interval.ms", Constants.SECOND_MS);
    MASTER_CLIENT_TIMEOUT_MS =
        getLongProperty("tachyon.user.master.client.timeout.ms", 10 * Constants.SECOND_MS);
    DEFAULT_BLOCK_SIZE_BYTE = getLongProperty("tachyon.user.default.block.size.byte", Constants.GB);
    REMOTE_READ_BUFFER_SIZE_BYTE =
        getIntProperty("tachyon.user.remote.read.buffer.size.byte", Constants.MB);
    USE_RDMA = getBooleanProperty("tachyon.user.remote.use.rdma", false);
  }
}
