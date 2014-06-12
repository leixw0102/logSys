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

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 17:17
 */
public interface Player {

	String COMMON_DATE_FORMAT1 = "yyyyMMdd HHmmss";
	String COMMON_DATE_FORMAT2 = "yyyy-MM-dd HH:mm:ss";
    String KEY_PLAYER_TIMELINE = "TimeLine";
    String KEY_PLAYER_OPERTYPE = "OperType";
    String KEY_PLAYER_PROGATHERID = "ProGatherID";
    String KEY_PLAYER_PROGRAMID = "ProgramID";
    String SPLIT="|";
    String EMPTY="";
	String SPLIT_T = "\t";
    String CONTENT_PREFIX="content=";
    String COMMA_SIGN = ",";
    String EQUAL_SIGN = "=";
	//DataSource系统来源1：易视腾2：云立方
	String DATA_SOURCE = "1";
	//EPGCodeEPG版本编号,见EPGCode版本编号表
	String EPG_CODE = "06";
	//Fsource数据来源，见数据来源表
	String F_SOURCE = "1";
    public List<String> getKeys();
}
