package tachyon.worker.jxio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;
import tachyon.worker.BlocksLocker;
import tachyon.worker.nio.DataServerMessage;

import com.mellanox.jxio.jxioConnection.JxioConnectionServer.Callbacks;

public class ServerCallBack implements Callbacks {
	private static final Logger LOG = LoggerFactory
			.getLogger(Constants.LOGGER_TYPE);

	private BlocksLocker mBlockLocker;

	public ServerCallBack(BlocksLocker blockLocker) {
		mBlockLocker = blockLocker;
	}

	@Override
	public void newSessionIS(URI arg0, InputStream arg1) {
		LOG.error("Illegal Operation",
				"newSessionIS operation is not supported.");
	}

	/**
	 * @param query
	 *            The query string such as name1=value1&name2=value2...
	 * @return Decoded query
	 */
	Map<String, String> decodeQueryString(String query) {
		String[] queries = query.split("&");
		Map<String, String> ret = new HashMap<String, String>(queries.length);
		for (String queryComponent : queries) {
			String[] pair = queryComponent.split("=");
			if (pair.length != 2) {
				throw new java.lang.IllegalArgumentException(
						"Query format wrong.");
			}
			ret.put(pair[0], pair[1]);
		}
		return ret;
	}

	@Override
	public void newSessionOS(URI uri, OutputStream os) {
		LOG.info("[DataServer] Request " + uri + " received.");
		String query = uri.getQuery();
		Map<String, String> params = decodeQueryString(query);
		if (!params.containsKey(JxioConstants.NAME_BLOCK_ID)
				|| !params.containsKey(JxioConstants.NAME_OFFSET)
				|| !params.containsKey(JxioConstants.NAME_LENGTH)) {
			LOG.error("Query string is not complete.");
		}
		long blockId = Long.valueOf(params.get(JxioConstants.NAME_BLOCK_ID));
		long offset = Long.valueOf(params.get(JxioConstants.NAME_OFFSET));
		long length = Long.valueOf(params.get(JxioConstants.NAME_LENGTH));

		LOG.debug(String.format("Transfering %s bytes at block %s.", length,
				blockId));

		try {
			if (offset < 0) {
				throw new IOException("Offset can not be negative: " + offset);
			}
			if (length < 0 && length != -1) {
				throw new IOException("Length can not be negative except -1: "
						+ length);
			}

			String filePath = CommonUtils.concat(WorkerConf.get().DATA_FOLDER,
					blockId);

			int lockId = mBlockLocker.lock(blockId);

			LOG.info("Try to response remote request by reading from "
					+ filePath);
			RandomAccessFile file = new RandomAccessFile(filePath, "r");

			long fileLength = file.length();
			String error = null;
			if (offset > fileLength) {
				error = String.format(
						"Offset(%d) is larger than file length(%d)", offset,
						fileLength);
			}
			if (error == null && length != -1 && offset + length > fileLength) {
				error = String
						.format("Offset(%d) plus length(%d) is larger than file length(%d)",
								offset, length, fileLength);
			}
			if (error != null) {
				file.close();
				throw new IOException(error);
			}
			if (length == -1) {
				length = fileLength - offset;
			}
			// FileChannel channel = file.getChannel();
			// MappedByteBuffer data =
			// channel.map(FileChannel.MapMode.READ_ONLY,
			// offset, length);

			byte[] content = new byte[(int) length + 10];
			int bytesReaded = file.read(content, (int) offset, (int) length);
			if (bytesReaded != length) {
				LOG.error("Want to read " + length
						+ " bytes but actually readed " + bytesReaded
						+ " bytes.");
			}
//			String contentStr = new String(content, 0, bytesReaded, "UTF8");
//			LOG.info("Content size " + bytesReaded + "; contentStr Size: " + contentStr.length());
//			LOG.info("CONTENT: **** " + contentStr);
			os.write(content, 0, bytesReaded);
			os.close();
			file.close();

			mBlockLocker.unlock(blockId, lockId);

			LOG.info("Successfully sending " + bytesReaded
					+ " bytes to the remote peer.");

		} catch (Exception e) {
			LOG.error("Failed to transfer requested data : " + e.getMessage(),
					e);
		}
	}
}
