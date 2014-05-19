package tv.icntv.log.crawl2.push;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.commons.lang.NullArgumentException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import tv.icntv.log.crawl2.commons.HttpClientUtil;

public class ActiveName {

	public static String activeMasterNameNode(String url,String ...nn) throws IOException{
		for(String n:nn){
			String content= HttpClientUtil.getContent(MessageFormat.format(url, n));
			if(null == content){
				throw new  NullPointerException(url);
			}
			String test=((JSONArray)JSON.parseObject(content).get("beans")).get(0).toString();
			if(null == test){
				throw new NullArgumentException(url+" content");
			}
			String state=JSON.parseObject(test).get("tag.HAState").toString();
			if(state.toLowerCase().equals("active")){
				return n;
			}
		}
		return "";
	}
}
