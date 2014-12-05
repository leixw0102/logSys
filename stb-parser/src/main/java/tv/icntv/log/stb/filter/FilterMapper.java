package tv.icntv.log.stb.filter;/*
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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.core.ParserConstant;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/19
 * Time: 11:38
 */
public class FilterMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
    String fileKeys="";
    Map<String,String> maps=Maps.newHashMap();
    private MultipleOutputs mos;
    private String otherPath="";
    private Logger logger= LoggerFactory.getLogger(getClass());
    private boolean isOutOther=true;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String path_prefix=context.getConfiguration().get(ParserConstant.OUTPUT_PREFIX);
        if(!path_prefix.endsWith(File.separator)){
            path_prefix+=File.separator;
        }
        fileKeys=context.getConfiguration().get(ParserConstant.OUTPUT_SUFFIX);

        for(String str:fileKeys.split(ParserConstant.FILTER_SPILTER)){
            String[]kv=str.split("-");
            String[] keys=kv[0].split(",");
            for(String key:keys){
                maps.put(key,path_prefix+kv[1]);
            }

        }
        String other=context.getConfiguration().get(ParserConstant.OTHER_PATH);
        if(Strings.isNullOrEmpty(other)){
            isOutOther=false;
        }else{
            otherPath=path_prefix+other;
        }
        mos= new MultipleOutputs(context);
       // super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if(null!=mos){
            mos.close();
        }
           //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null == value){
            return;
        }
        List<String> lineSplit=Lists.newArrayList(Splitter.on(ParserConstant.STB_SPLITER).limit(16).split(value.toString()));
        if(!check(lineSplit)){
            logger.info("regular error line ...\t"+value.toString());
            return;
        }
        Text out=new Text(Joiner.on("\t").join(lineSplit));
        String module=lineSplit.get(13);
        if(maps.containsKey(module)){
            String v=maps.get(module);
            mos.write(NullWritable.get(),out,v);
            return;
        }
        if(isOutOther){
            mos.write(NullWritable.get(),out,otherPath);
        }
        return;
    }

    private boolean check(List<String> lineSplit) {
        if(lineSplit==null||lineSplit.isEmpty() || lineSplit.size()!=16){
            return false;
        }
        // TODO Auto-generated method stub
        return lineSplit.get(0).matches("\\d{36}") && lineSplit.get(2).matches("(\\d*)") && lineSplit.get(3).matches("\\d{15}")
                //&&lineSplit.get(7).matches("[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}")
                //&&lineSplit.get(7).matches("([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3},?).*")
                &&lineSplit.get(8).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(9).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(10).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(11).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(12).matches("\\d{1}")
                &&lineSplit.get(13).matches("\\d*")
                &&lineSplit.get(14).matches("[A-Za-z]*")
                ;
    }
}
