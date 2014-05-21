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

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import tv.icntv.log.stb.core.ParserConstant;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/20
 * Time: 11:26
 */
public class Test {
    public static void main(String[]args){
        String str="800-userLogin/{0,number,#0000000000000}/userLogin;900-contentView/{0,number,#0000000000000}/contentView;100-devicePlayer/{0,number,#0000000000000}/devicePlayer;101,102,103,104,105,106,107-logEpg/{0,number,#0000000000000}/logEpg";
//        MessageFormat mf=MessageFormat
        List<String> files= Lists.transform(Lists.newArrayList(Splitter.on(ParserConstant.FILTER_SPILTER).split(str)), new Function<String,String>() {
            @Override
            public String apply(java.lang.String input) {
                return MessageFormat.format(input, System.currentTimeMillis());  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        System.out.println(new Date(System.currentTimeMillis()).toLocaleString());
        System.out.println(files);
        Map<String,String> maps  = Maps.newHashMap();
        for(String str1:files){
            String[]kv=str1.split("-");
            String[] keys=kv[0].split(",");
            for(String key:keys){
                maps.put(key,kv[1]);
            }

        }
//        System.out.println(maps);
//        System.out.println(MessageFormat.format("other/{0,number,#0000000000000}/other",System.currentTimeMillis()));
        String st1=maps.get("800");
         System.out.println(st1.substring(st1.lastIndexOf("/")+1));
    }
    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
