package tv.icntv.log.stb.contentview;/*
 * Copyright 2014 Future TV, Inc.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.icntv.tv/licenses/LICENSE-1.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.ReflectUtils;
import tv.icntv.log.stb.commons.SplitterIcntv;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/21
 * Time: 14:28
 */
public class ContentViewMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements ContentView{

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
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
        ContentViewDomain view = new ContentViewDomain();
        Map<String,String> maps = SplitterIcntv.toMap(Splitter.on(COMMA_SIGN).limit(17).split(content),EQUAL_SIGN);//.withKeyValueSeparator("=").split(content);

        for(String k : getKeys()){
            try {
                ReflectUtils.setFieldValue(view.getClass().getDeclaredField(k), view, new String[]{maps.get(k)});
            } catch (NoSuchFieldException e) {
                logger.error("reflect error",e);
            }
        }

        context.write(NullWritable.get(),new Text(view.toString()));

    }

    @Override
    public List<String> getKeys() {
        return Lists.newArrayList("deviceCode","catgId","startDate","endReason","endDate","videoType","programId","chargeType","outerCode");  //To change body of implemented methods use File | Settings | File Templates.
    }
}
