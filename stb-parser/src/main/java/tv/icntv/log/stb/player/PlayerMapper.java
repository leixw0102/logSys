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

import java.io.IOException;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 15:56
 */
public class PlayerMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements Player{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null == value|| Strings.isNullOrEmpty(value.toString())){
            return;
        }
        String[] values= value.toString().split(SPLIT_T);
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

	    if(contentArr==null || contentArr.length<=0 || contentArr.length!=4){
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
	   String timeline = StringUtils.substringAfter(contentArr[3].trim(), EQUAL_SIGN);
	    if(timeline.matches("\\d*")){
		    playerLogDomain.setTimeLine(timeline);
	    }else{
		    return;
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
