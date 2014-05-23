package tv.icntv.log.stb.core;/*
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

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/16
 * Time: 16:07
 */
public interface ParserConstant {


    public String RULE_FILE="ruleFile";
    //rule key

    public String INPUT="stb.input.path";
    public String BACK="stb.back.path";
    public String OUTPUT_PREFIX="stb.filter.result.path";
    public String OUTPUT_SUFFIX="stb.filter.file.keys";
    public String FILTER_JOB_PATHS="stb.filter.job.paths";
//    public String FILTER_KEYS="stb.filter.rule.keys";
    public String FILTER_SPILTER=";";
    public String FILTER_TIME="time_long";
    public String OTHER_PATH="stb.filter.file.other";
    public String DAY_CONSTANT="current_day";


    public String STB_SPLITER="/";
    public String DAY_YYYY_MM_DD="yyyy-MM-dd";

    public String file_success_suffix=".writed";
    public String parseing_suffix=".parsing";
    public String parsed_suffix=".parsed";



    //userlogin
    public String USER_LOGIN_JOB_INPUT="stb.parser.user.login.relative.input";
    public String USER_LOGIN_JOB_OUTPUT="stb.parser.user.login.hadoop.output";
    public String USER_LOGIN_DAT_OUTPUT="stb.parser.user.login.dat.output";

    public String PLAYER_JOB_INPUT="stb.parser.device.player.relative.input";
    public String PLAYER_JOB_OUTPUT="stb.parser.device.player.hadoop.output";
    public String PLAYER_DAT_OUTPUT="stb.parser.device.player.dat.result";
}
