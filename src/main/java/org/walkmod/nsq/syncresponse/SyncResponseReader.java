package org.walkmod.nsq.syncresponse;

import org.walkmod.nsq.Message;
import org.walkmod.nsq.NSQReader;
import org.walkmod.nsq.exceptions.RequeueWithoutBackoff;
import org.walkmod.nsq.lookupd.AbstractLookupd;
import org.walkmod.nsq.lookupd.BasicLookupd;

public class SyncResponseReader extends NSQReader {
	
	private SyncResponseHandler handler;
	
	public SyncResponseReader(String topic, String channel, SyncResponseHandler handler) {
		super();
		this.handler = handler;
		this.init(topic, channel);
	}

	private class SyncResponseMessageRunnable implements Runnable {
		
		public SyncResponseMessageRunnable(Message msg) {
			super();
			this.msg = msg;
		}

		private Message msg;

		public void run() {
			boolean success = false;
			boolean doDelay = true;
			try{
				success = handler.handleMessage(msg);
			}catch(RequeueWithoutBackoff e){
				doDelay = false;
			}catch(Exception e){
				// do nothing, success already false
			}
			
			// tell conn about success or failure
			if(success){
				finishMessage(msg);
			}else{
				requeueMessage(msg, doDelay);
			}
		}
	}

	@Override
	protected Runnable makeRunnableFromMessage(Message msg) {
		return new SyncResponseMessageRunnable(msg);
	}

}
