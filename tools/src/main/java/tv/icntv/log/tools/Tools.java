package tv.icntv.log.tools;/*
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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/26
 * Time: 10:34
 */
public class Tools {
    private static Options init() {
        Options options = new Options();
        options.addOption("h", false, "help me");
        options.addOption("I",  true, "input hdfs source file");
        options.addOption("R",true,"filter regular");
        options.addOption("Out",true,"dat output file");
        return options;
    }
    private static void help(){
        System.out.println("-I input -Out output");
    }
    public static void main(String[]args) throws ParseException, IOException {

        CommandLineParser parser = new PosixParser();
        CommandLine line= parser.parse(init(),args);
        Api api = new FileApi();
        long time=System.nanoTime();
        String input = line.getOptionValue("I");
        List<String> inputStr = Lists.newArrayList(Splitter.on(",").omitEmptyStrings().split(input));
//        List<Path> paths = Lists.transform(inputStr,new Function<String, Path>() {
//            @Override
//            public Path apply(java.lang.String input) {
//                System.out.println("input ="+input);
//                try{
//                Path path = new Path(input);  //To change body of implemented methods use File | Settings | File Templates.
//                }catch (Exception e){
//                    return null;
//                }
//                if()
//            }
//        });
        FileSystem fileSystem = FileSystem.get(new Configuration());
        List<Path> paths = Lists.newArrayList();
        for(String str : inputStr){
            Path path = null;
            try {
                path = new Path(str) ;
                if(fileSystem.exists(path)){
                    System.out.println(" add path .."+str);
                    paths.add(path);
                }
            }catch (Exception e){
                continue;
            }

        }
        if(null == paths || paths.isEmpty()){
            System.out.println("input 2 path null");
        }
        String regular = line.getOptionValue("R");
        if(Strings.isNullOrEmpty(regular)){
            regular = "part-m-\\d*.lzo";
        }
        System.out.println("regular = "+regular);
        boolean test=api.writeDat(paths.toArray(new Path[paths.size()]),regular,new Path(line.getOptionValue("Out")));
        System.out.println(test+"\t"+(System.nanoTime()-time)/Math.pow(10,9));
    }
}
