/*
 * Copyright 2014 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import tv.icntv.log.stb.player.Player;
import tv.icntv.log.stb.player.PlayerLogDomain;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/05/26
 * Time: 09:47
 */
public class Test1 implements Player {
	public static void main(String[] args) {

		List<String> lines= null;
		try {
			lines = Files.readLines(new File("C:\\Users\\lenovo\\Downloads\\devicePlayer-m-00023"), Charsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
		System.out.println(lines.size());
		for(String value1 : lines){
			String commonLoggerStr=value1.toString();

			if(null == commonLoggerStr|| Strings.isNullOrEmpty(commonLoggerStr.toString())){
				return;
			}
			String[] values= commonLoggerStr.toString().split(SPLIT_T);
			if(null == values || values.length!=16){
				return;
			}
			String content=values[15];
			if(Strings.isNullOrEmpty(content)){
				return;
			}
			if(content.startsWith(CONTENT_PREFIX)){
				content=content.replace(CONTENT_PREFIX,"");
			}

			String[] contentArr = content.split(COMMA_SIGN);

			if(contentArr==null || contentArr.length<=0){
				System.out.println("contentArr为空");
				return ;
			}
			PlayerLogDomain playerLogDomain=new PlayerLogDomain();
//	    StringBuffer stringBuffer=new StringBuffer();
			//playId播放id：每一次播放（从开始到结束）的唯一识别编号
			//CNTVID用户序列号
//	    stringBuffer.append(StringsUtils.getEncodeingStr(values[3])).append(SPLIT);
			playerLogDomain.setIcntvId(values[3]);
			//Timeline操作时间轴
//	    stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[3].trim(), EQUAL_SIGN))).append(SPLIT);
			String timeline=StringUtils.substringAfter(contentArr[3].trim(), EQUAL_SIGN);
			if(timeline.matches("\\d*")){
			playerLogDomain.setTimeLine(timeline);
			}else {
//				System.out.println(content);
				continue;
			}
			//OperType操作类型标识：11-播放开始21-播放结束12-快进开始22-快进结束13-后退开始23-后退结束14-暂停开始24-暂停结束15-缓冲开始25-缓冲结束16-拖动开始26-拖动结束99-播放错误
//	    stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))).append(SPLIT);
			playerLogDomain.setOperType(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN));
			//OpTime操作时间。格式是：YYYYMMDDHH24MISS
//	    stringBuffer.append(StringsUtils.getEncodeingStr(values[11])).append(SPLIT);
			playerLogDomain.setOpTime(values[11]);
			//DataSource系统来源1：易视腾2：云立方
//	    stringBuffer.append(DATA_SOURCE).append(SPLIT);
			//EPGCodeEPG版本编号,见EPGCode版本编号表
//	    stringBuffer.append(EPG_CODE).append(SPLIT);
			//Fsource数据来源，见数据来源表
//	    stringBuffer.append(F_SOURCE).append(SPLIT);
			//ProGatherID节目集ID
//	    stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN))).append(SPLIT);
			playerLogDomain.setProGatherId(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
			//ProgramID节目ID
//	    stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN))).append(SPLIT);
			playerLogDomain.setProgramId(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN));
			//RemoteControl遥控设备类型
//	    stringBuffer.append(EMPTY).append(SPLIT);
			//resolution 视频码率：1.高清2.标清
//	    stringBuffer.append(EMPTY).append(SPLIT);
			//Reserved1保留字段1
//	    stringBuffer.append(EMPTY).append(SPLIT);
			//Reserved2保留字段2
//	    stringBuffer.append(EMPTY);

			System.out.println(playerLogDomain.toString());
		}

	}

	@Override
	public List<String> getKeys() {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
