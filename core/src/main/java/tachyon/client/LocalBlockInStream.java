package tachyon.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * BlockInStream for local block.
 */
public class LocalBlockInStream extends BlockInStream {
  private TachyonByteBuffer mTachyonBuffer = null;
  private ByteBuffer mBuffer = null;

  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the file
   * @param buf the buffer of the whole block in local memory
   * @throws IOException
   */
  LocalBlockInStream(TachyonFile file, ReadType readType, int blockIndex, TachyonByteBuffer buf)
      throws IOException {
    super(file, readType, blockIndex);

    mTachyonBuffer = buf;
    mBuffer = mTachyonBuffer.DATA;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      mTachyonBuffer.close();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    if (mBuffer.remaining() == 0) {
      close();
      return -1;
    }
    return mBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int ret = Math.min(len, mBuffer.remaining());
    if (ret == 0) {
      close();
      return -1;
    }
    mBuffer.get(b, off, ret);
    return ret;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }
    mBuffer.position((int) pos);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    int ret = mBuffer.remaining();
    if (ret > n) {
      ret = (int) n;
    }
    mBuffer.position(mBuffer.position() + ret);
    return ret;
  }
}
