/* Copyright 2013 Future TV, Inc.
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
package tv.icntv.log.stb.cdnadapter;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/07/21
 * Time: 09:53
 */
public interface CdnAdapter {
    String SPLIT_T = "\t";
    String MODULE="PlayQos";
    String URLID_SPLIT = ":Url=";
    String URLID_SPLIT_2 = ": Url=";
    String EQUAL_SIGN="=";
    String AND = "&";



    String id="id";
    String Url="Url";
    String BuffCount="BuffCount";
    String BuffAverTimeCost="BuffAverTimeCost";
    String OpenTimeCost="OpenTimeCost"; //视频加载时长(ms)
    String SeekCount="SeekCount";// 视频快进快退次数(ms)
    String SeekTimeAverTimeCost="SeekTimeAverTimeCost";//视频快进快退平均加载时长(ms)
    String PlayTotalTimeCost="PlayTotalTimeCost";//总共播放的时间（MS）
    String Error="Error";//播放器抛出的error信息值，若无误，则值为no
}
