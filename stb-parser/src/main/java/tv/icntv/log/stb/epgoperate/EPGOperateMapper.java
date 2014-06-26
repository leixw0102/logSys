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
package tv.icntv.log.stb.epgoperate;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.log.stb.player.Player;

import java.io.IOException;
import java.util.List;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/06/20
 * Time: 11:11
 */
public class EPGOperateMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements EPGOperate {

	/**
	 *
	 * @param key
	 * @param value 字符串例子及对应的字段描述如下（括号中为字段名称及描述）：
	 *      010121008091051201406170359240060001（log_id	日志编号）
	 *      872（seq_id	请求编号）
	 *      0（retry_seqid	重试请求编号）
	 *      010121008091051（device_id	设备编号）
	 *      4.3.5.13-JX:1（version_id	终端版本号）
	 *      JX_E3E420140513001（platform_id	平台号）
	 *      0c:c6:55:10:64:73（mac	终端mac）
	 *      123.75.39.185（ip	终端ip）
	 *      2014-06-17 04:00:36 700（req_time	终端发送日志请求的时间）
	 *      2014-06-17 03:59:40 218（cur_time	日志产生时的终端时间）
	 *      2014-06-17 03:58:27 524（client_time	日志产生时的服务器时间）(operate time暂取这个)
	 *      2014-06-17 03:59:24 006（server_time	接收到日志时的服务器时间）
	 *      1（level	日志等级）
	 *      106（module	模块）
	 *      AppLog（action	操作）
	 *      PageName=EPGHistory,EPGID=968228,DatePoint=0（content	日志内容）
	 *
	 *
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(null == value|| Strings.isNullOrEmpty(value.toString())){
			return;
		}
		String[] values= value.toString().split(SPLIT_T);
		if(null == values || values.length!=16){
			return;
		}
		//日志内容，例：PageName=EPGHistory,EPGID=968228,DatePoint=0
		String content=values[15].trim();
		if(Strings.isNullOrEmpty(content)){
			return;
		}

		if(content.startsWith(CONTENT_PREFIX)){
			content=content.replace(CONTENT_PREFIX,"");
		}

		String[] contentArr = content.split(COMMA_SIGN);

		if(contentArr==null || contentArr.length<=0 || contentArr.length!=4){
			System.out.println("contentArr为空");
			return ;
		}

		EPGOperateDomain epgOperateDomain = new EPGOperateDomain();

		//用户序列号
		epgOperateDomain.setCntvid(values[3].trim());

		//EPG页面ID
		//TODO(空)
		epgOperateDomain.setEpgPageId(EMPTY);

		//源对象类型：1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
		//TODO(空)
		epgOperateDomain.setSourceObjectType(EMPTY);

		//源对象ID
		//TODO（空）
		epgOperateDomain.setSourceObjectId(EMPTY);


		if(PAGE_NAME_EPG_HISTORY.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("2");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("1");
		}else if(PAGE_NAME_EPG_SEARCH.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("1");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(EMPTY);
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("21");
		}else if(PAGE_NAME_ALBUM.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("5");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(EMPTY);
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("24");
		}else if(PAGE_NAME_PANEL.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("1");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(EMPTY);
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("11");
		}else if(PAGE_NAME_EPG_DETAIL.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
//			if(!contentArr[1].trim().matches("\\d+")){
//				return;//节目id只能包含数字否则弃用本条日志
//			}
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("2");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("11");
		}else if(PAGE_NAME_WEB_INDEX.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("1");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(EMPTY);
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("11");
		}else if(PAGE_NAME_EPG_LIST.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
			if(!contentArr[1].trim().matches("\\d+")){
				return;//节目id只能包含数字否则弃用本条日志
			}
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType("4");
			//目标对象ID
			epgOperateDomain.setTargetObjectId(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("11");
		}else{
			//TODO 测试，之后直接return
			//目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
			epgOperateDomain.setTargetObjectType(EMPTY);
			//目标对象ID
			epgOperateDomain.setTargetObjectId(EMPTY);
			//搜索关键词:当操作类型为搜索时该字段需要传值
			epgOperateDomain.setKeyword(EMPTY);
			//操作类型标识 1-添加收藏,2-添加书签,11-跳转进入,12-跳转返回(保留),21-搜索,22-用户注册,23-在线帮助,24-专题,25-播放,30-退出,31-异常退出
			epgOperateDomain.setOperType("-1");
//			return;
		}


		//操作时间。格式是：YYYYMMDD HH24MISS
		epgOperateDomain.setOpTime(values[10].trim());

		//系统来源 1：易视腾,2：云立方,3：小米
		epgOperateDomain.setDataSource(DATA_SOURCE);

		//数据来源，见数据来源表
		epgOperateDomain.setFsource(F_SOURCE);

		//EPG版本编号,见EPGCode版本编号表
		epgOperateDomain.setEPGCode(EPG_CODE);

		//遥控设备类型 1. 遥控器,2. 多屏设备(若不能取到该数据则为空)
		epgOperateDomain.setRemoteControl(EMPTY);

		context.write(NullWritable.get(),new Text(epgOperateDomain.toString()));
	}

	@Override
	public List<String> getKeys() {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
