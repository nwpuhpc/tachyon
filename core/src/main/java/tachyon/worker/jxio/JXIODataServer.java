package tachyon.worker.jxio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.mellanox.jxio.ServerPortal;
import com.mellanox.jxio.jxioConnection.JxioConnectionServer;

import tachyon.Constants;
import tachyon.conf.CommonConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;

/**
 * This data server runs on network that supports RDMA, it will take advantage
 * of RDMA
 *
 */
public class JXIODataServer implements DataServer {
	private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
	private InetSocketAddress mAddress;
	private BlocksLocker mBlockLocker;
	private JxioConnectionServer mServer;
	private int mPort;
	private boolean mIsClosed = false;
	
	
	public JXIODataServer(InetSocketAddress address, BlocksLocker locker) {
		String uriString = String.format("rdma://%s:%s", address.getAddress().getHostAddress(), address.getPort());
	    LOG.info("[JXIO] Starting DataServer @ " + uriString);
	    CommonConf.assertValidPort(address);
	    mAddress = address;
	    mBlockLocker = locker;

		try {
		    URI uri = new URI(uriString);
		    //TODO Parameterize the numworkers.
			mServer = new JxioConnectionServer(uri, 4, new ServerCallBack(mBlockLocker));
			URI listenerUri = mServer.getListenerUri();
			mPort = listenerUri.getPort();
			LOG.info("DataServer @ Port "+mPort);
			mServer.start();
		} catch (URISyntaxException e1) {
			LOG.error("URI Syntax Error", e1);
		}
	}

	@Override
	public void close() throws IOException {
		mServer.disconnect();
		mIsClosed = true;
	}

	@Override
	public int getPort() {
		return mPort;
	}

	@Override
	public boolean isClosed() {
		return mIsClosed;
	}

}
