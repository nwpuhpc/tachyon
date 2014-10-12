package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.jxio.JxioConstants;

import com.mellanox.jxio.jxioConnection.JxioConnection;

public class RemoteRDMABlockInStream extends BlockInStream {
	private static final int BUFFER_SIZE = UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE;
	private static final Logger LOG = LoggerFactory
			.getLogger(Constants.LOGGER_TYPE);

	private ClientBlockInfo mBlockInfo;
	private String mHost;
	private int mPort;
	private boolean mRecache = true;

	private JxioConnection conn = null;
	private InputStream inputStream = null;

	private Object mUFSConf = null;

	/**
	 * @param file
	 *            the file the block belongs to
	 * @param readType
	 *            the InStream's read type
	 * @param blockIndex
	 *            the index of the block in the file
	 * @throws IOException
	 */
	RemoteRDMABlockInStream(TachyonFile file, ReadType readType, int blockIndex)
			throws IOException {
		this(file, readType, blockIndex, null);
	}

	/**
	 * @param file
	 *            the file the block belongs to
	 * @param readType
	 *            the InStream's read type
	 * @param blockIndex
	 *            the index of the block in the file
	 * @param ufsConf
	 *            the under file system configuration
	 * @throws IOException
	 */
	RemoteRDMABlockInStream(TachyonFile file, ReadType readType,
			int blockIndex, Object ufsConf) throws IOException {
		super(file, readType, blockIndex);

		mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);

		if (!mFile.isComplete()) {
			throw new IOException("File " + mFile.getPath()
					+ " is not ready to read");
		}

		mRecache = readType.isCache();

		mBlockInfo = file.getClientBlockInfo(blockIndex);
		List<NetAddress> blockLocations = mBlockInfo.getLocations();
		LOG.info("Block locations:" + blockLocations);

		for (int k = 0; k < blockLocations.size(); k++) {
			String host = blockLocations.get(k).mHost;
			int port = blockLocations.get(k).mSecondaryPort;

			// The data is not in remote machine's memory if port == -1.
			if (port == -1) {
				continue;
			}
			if (host.equals(InetAddress.getLocalHost().getHostName())
					|| host.equals(InetAddress.getLocalHost().getHostAddress())
					|| host.equals(NetworkUtils.getLocalHostName())) {
				String localFileName = CommonUtils.concat(
						mTachyonFS.getLocalDataFolder(), mBlockInfo.blockId);
				LOG.warn("Master thinks the local machine has data "
						+ localFileName + "! But not!");
			}
			LOG.info(host + ":" + port + " current host is "
					+ NetworkUtils.getLocalHostName() + " "
					+ NetworkUtils.getLocalIpAddress());
			mHost = host;
			mPort = port;
			break;
		}

		conn = connectRdma(new InetSocketAddress(mHost, mPort),
				mBlockInfo.blockId, mBlockInfo.offset, mBlockInfo.length);
		inputStream = conn.getInputStream();

		mUFSConf = ufsConf;

		// if (inputStream.) {
		// setupStreamFromUnderFs(mBlockInfo.offset, mUFSConf);
		//
		// if (mCheckpointInputStream == null) {
		// mTachyonFS.reportLostFile(mFile.mFileId);
		//
		// throw new IOException("Can not find the block " + mFile + " "
		// + mBlockIndex);
		// }
		// }
	}

	@Override
	public void close() throws IOException {
		mClosed = true;
	}

	@Override
	public int read() throws IOException {
		int ret = inputStream.read() & 0xFF;
		return ret;
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

		int ret = inputStream.read(b, off, len);
		return ret;
	}

	private JxioConnection connectRdma(InetSocketAddress address, long blockId,
			long offset, long length) throws IOException {
		String uriString = String.format("rdma://%s:%s/data?%s=%s&%s=%s&%s=%s",
				address.getAddress().getHostAddress(), address.getPort(),
				JxioConstants.NAME_BLOCK_ID, blockId,
				JxioConstants.NAME_LENGTH, length, JxioConstants.NAME_OFFSET,
				offset);
		LOG.info("Requesting data from " + uriString);
		JxioConnection connection = null;
		try {
			URI uri = new URI(uriString);
			connection = new JxioConnection(uri);
			return connection;
		} catch (URISyntaxException e) {
			LOG.error("URI syntax error.");
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void seek(long pos) throws IOException {
		LOG.error("Unimplemented: seek");
	}

	@Override
	public long skip(long n) throws IOException {
		return inputStream.skip(n);
	}
}
