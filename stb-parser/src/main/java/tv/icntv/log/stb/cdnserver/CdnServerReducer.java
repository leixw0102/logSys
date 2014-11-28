package tv.icntv.log.stb.cdnserver;/*
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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 14:34
 *  * key = icntv+ip  + url
 * value=  ip sliceSize status  time  useragent
 */
public class CdnServerReducer extends Reducer<Text,Text,NullWritable,Text> {
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,String> maps = new HashMap<String, String>(); //Maps.newHashMap();
        for (Text text :values){
            String temp = text.toString();
            String[] vs = temp.split("\\|");
            if(null == vs || vs.length !=5){
                continue;
            }
            String tempKey=vs[0].trim()+"|"+vs[1].trim()+"|"+vs[2].trim();
            if(maps.containsKey(tempKey)){
                continue;
            }
            maps.put(tempKey,vs[3]+"|"+vs[4]);//+"|"+vs[0]);
        }
        CdnServer server = new CdnServer();
        Set<String> sets=maps.keySet();
        for(String str:sets){
           CdnServer temp = server.clone();
            temp.parser(key.toString()+"|"+str+"|"+maps.get(str));
            context.write(NullWritable.get(),new Text(temp.toString()));
        }
    }
}
