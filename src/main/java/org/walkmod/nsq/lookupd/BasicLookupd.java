package org.walkmod.nsq.lookupd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicLookupd extends AbstractLookupd {
	private static final Logger log = LoggerFactory
			.getLogger(AbstractLookupd.class);

	@Override
	public List<String> query(String topic) {
		String urlString = this.addr + "/lookup?topic=" + topic;
		URL url = null;
		HttpURLConnection con = null;
		InputStream is = null;
		List<String> producers = null;
		try {
			url = new URL(urlString);
			con = (HttpURLConnection) url.openConnection();
			is = con.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			producers = parseResponseForProducers(br);
		} catch (MalformedURLException e) {
			log.error("Malformed Lookupd URL: {}", urlString);
		} catch (IOException e) {
			if (con != null) {
				try {
					if (con.getResponseCode() != 500) {
						log.error("Problem reading lookupd response: ", e);
					}
				} catch (IOException e1) {
				}
			}
		}
		finally{
			if(is != null){
				try {
					is.close();
				} catch (IOException e) {}
			}
			if (con != null){
				con.disconnect();
			}
		}
		if(producers == null){
			producers = new LinkedList<String>();
		}
		return producers;
	}

	public BasicLookupd(String addr) {
		this.addr = addr;
	}

}
