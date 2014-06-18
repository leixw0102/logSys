package tv.icntv.log.stb.player;/*
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.util.DateUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 15:56
 */
public class PlayerMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements Player{
	/**
	 *
	 * @param key
	 * @param value 字符串例子及对应的字段描述如下（括号中为字段名称及描述）：
	 *      014444000014031201405010501059180038（log_id	日志编号）
	 *		3188（seq_id	请求编号）
	 *		0（retry_seqid	重试请求编号）
	 *		014444000014031（device_id	设备编号）
	 *   	:（version_id	终端版本号）
	 *		2720130807163420732（platform_id	平台号）
	 *		24:69:A5:7B:CE:99（mac	终端mac）
	 *		183.12.65.153（ip	终端ip）
	 *		2014-05-01 05:01:05 996（req_time	终端发送日志请求的时间）
	 *		2014-05-01 04:58:40 056（cur_time	日志产生时的终端时间）
	 *		2014-05-01 04:58:39 978（client_time	日志产生时的服务器时间）
	 *		2014-05-01 05:01:05 918（server_time	接收到日志时的服务器时间）
	 *		1（level	日志等级）
	 *		100（module	模块）
	 *		PlayLog（action	操作）
	 *   	content=OperType=05,ProGatherID=942407,ProgramID=6890867,TimeLine=1926120（content	日志内容）
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
	    //日志内容，例：content=OperType=05,ProGatherID=942407,ProgramID=6890867,TimeLine=1926120
        String content=values[15];
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
        PlayerLogDomain playerLogDomain=new PlayerLogDomain();
	    //playId播放id：每一次播放（从开始到结束）的唯一识别编号
	    //CNTVID用户序列号
        playerLogDomain.setIcntvId(values[3]);
		//Timeline操作时间轴(相对时间)
	   String timeline = StringUtils.substringAfter(contentArr[3].trim(), EQUAL_SIGN);
	    if(timeline.matches("\\d*")){
		    playerLogDomain.setTimeLine(timeline);
	    }else{
		    return;
	    }
	    //OperType操作类型标识：11-播放开始21-播放结束12-快进开始22-快进结束13-后退开始23-后退结束14-暂停开始24-暂停结束15-缓冲开始25-缓冲结束16-拖动开始26-拖动结束99-播放错误
        playerLogDomain.setOperType(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN));
	    //操作时间
        playerLogDomain.setOpTime( values[11]);
	    //ProGatherID节目集ID
        playerLogDomain.setProGatherId(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN));
	    //ProgramID节目ID
        playerLogDomain.setProgramId(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN));

	    context.write(NullWritable.get(),new Text(playerLogDomain.toString()));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	    String s = "014444000014031201405010501059180038    3188    0       014444000014031 :       2720130807163420732     24:69:A5:7B:CE:99       183.12.65.153   2014-05-01 05:01:05 996 2014-05-0\n" +
			    "1 04:58:40 056  2014-05-01 04:58:39 978 2014-05-01 05:01:05 918 1       100     PlayLog content=OperType=05,ProGatherID=942407,ProgramID=6890867,TimeLine=1926120";
        super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getKeys() {
        return Lists.newArrayList(KEY_PLAYER_TIMELINE,KEY_PLAYER_OPERTYPE,KEY_PLAYER_PROGATHERID,KEY_PLAYER_PROGRAMID);
    }

}
