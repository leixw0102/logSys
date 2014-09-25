package tv.icntv.log.stb.cdnadapter;/*
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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.StringsUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/07/21
 * Time: 09:43
 */
public class CdnAdapterMapper  extends Mapper<LongWritable,Text,NullWritable,Text> implements CdnAdapter{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null ==value){
            return ;
        }
        List<String> values= Lists.newArrayList(Splitter.on(SPLIT_T).split(value.toString()));
        if(null == values|| values.isEmpty() || values.size()!=16){
            return ;
        }
        String module=values.get(14).toLowerCase();
        if(!module.equals(MODULE.toLowerCase())){
            return;
        }

        String content=values.get(15);
        if(Strings.isNullOrEmpty(content)){
            return;
        }
        CdnAdapterDomain cdnAdapterDomain=new CdnAdapterDomain();
        cdnAdapterDomain.setIcntvId(values.get(3));
        cdnAdapterDomain.setDate(DateUtils.getFormatDate(values.get(10),"yyyy-MM-dd HH:mm:ss"));
        String urlIdStr = "";
        if (content.contains(URLID_SPLIT)) {
            urlIdStr = StringUtils.substringBefore(content, URLID_SPLIT);
        } else if (content.contains(URLID_SPLIT_2)) {
            urlIdStr = StringUtils.substringBefore(content, URLID_SPLIT_2);
        } else {
            logger.warn("no url_id split flag in : {}", content);
        }
        cdnAdapterDomain.setId(urlIdStr);
        cdnAdapterDomain.setUrl(StringsUtils.getLogParam(
                Url, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setOpenTimeCost(StringsUtils.getLogParam(
                OpenTimeCost, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setBuffCount(StringsUtils.getLogParam(
                BuffCount, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setBuffAverTimeCost(StringsUtils.getLogParam(
                BuffAverTimeCost, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setSeekCount(StringsUtils.getLogParam(
                SeekCount, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setSeekTimeAverTimeCost(StringsUtils.getLogParam(
                SeekTimeAverTimeCost, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setPlayTotalTimeCost(StringsUtils.getLogParam(
                PlayTotalTimeCost, EQUAL_SIGN,
                AND, content));
        cdnAdapterDomain.setError(StringsUtils.getLogParam(
                Error, EQUAL_SIGN,
                AND, content));
        context.write(NullWritable.get(),new Text(cdnAdapterDomain.toString()));
    }
}
