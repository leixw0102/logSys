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

import com.google.common.collect.Maps;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 14:34
 */
public class CdnServerReducer extends Reducer<Text,Text,NullWritable,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Map<String,String> maps = Maps.newHashMap();
        while (it.hasNext()){
            String temp = it.next().toString();
            String[] vs = temp.split("\\|");
            String tempKey=vs[0].trim()+"|"+vs[1].trim();
            if(maps.containsKey(tempKey)){
                continue;
            }
            maps.put(tempKey,vs[2]+"|"+vs[3]);
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
