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

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;

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
        options.addOption("Out",true,"dat output file");
        return options;
    }
    private static void help(){
        System.out.println("-I input -Out output");
    }
    public static void main(String[]args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        CommandLine line= parser.parse(init(),args);
        Api api = new FileApi();
        long time=System.nanoTime();
        boolean test=api.writeDat(new Path(line.getOptionValue("I")),new Path(line.getOptionValue("Out")));
        System.out.println(test+"\t"+(System.nanoTime()-time)/Math.pow(10,9));
    }
}
