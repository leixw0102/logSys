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

import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.StringsUtils;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/24
 * Time: 03:18
 */
public class PlayerLogDomain {
    private String playId="";
    private String icntvId="";
    private String timeLine="";
    private String operType="";
    private String opTime="";
    private String dataSource="1";
    private String epgCode="06";
    private String fSource="1";
    private String proGatherId="";
    private String programId;
    private String remoteControl="1";
    private String resolution="";
    private String reserved1="";
    private String reserved2="";

    @Override
    public String toString() {

        StringBuffer sb=new StringBuffer();
        return sb.append(StringsUtils.getEncodeingStr(this.getPlayId())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getIcntvId())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getTimeLine())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getOperType())).append("|")
                .append(StringsUtils.getEncodeingStr(DateUtils.getFormatDate(this.getOpTime()))).append("|")
                .append(StringsUtils.getEncodeingStr(this.getDataSource())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getEpgCode())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getfSource())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getProGatherId())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getProgramId())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getRemoteControl())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getResolution())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getReserved1())).append("|")
                .append(StringsUtils.getEncodeingStr(this.getReserved2())).toString();
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getEpgCode() {
        return epgCode;
    }

    public void setEpgCode(String epgCode) {
        this.epgCode = epgCode;
    }

    public String getfSource() {
        return fSource;
    }

    public void setfSource(String fSource) {
        this.fSource = fSource;
    }

    public String getIcntvId() {
        return icntvId;
    }

    public void setIcntvId(String icntvId) {
        this.icntvId = icntvId;
    }

    public String getOperType() {
        return operType;
    }

    public void setOperType(String operType) {
        this.operType = operType;
    }

    public String getOpTime() {
        return opTime;
    }

    public void setOpTime(String opTime) {
        this.opTime = opTime;
    }

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public String getProGatherId() {
        return proGatherId;
    }

    public void setProGatherId(String proGatherId) {
        this.proGatherId = proGatherId;
    }

    public String getProgramId() {
        return programId;
    }

    public void setProgramId(String programId) {
        this.programId = programId;
    }

    public String getRemoteControl() {
        return remoteControl;
    }

    public void setRemoteControl(String remoteControl) {
        this.remoteControl = remoteControl;
    }

    public String getReserved1() {
        return reserved1;
    }

    public void setReserved1(String reserved1) {
        this.reserved1 = reserved1;
    }

    public String getReserved2() {
        return reserved2;
    }

    public void setReserved2(String reserved2) {
        this.reserved2 = reserved2;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getTimeLine() {
        return timeLine;
    }

    public void setTimeLine(String timeLine) {
        this.timeLine = timeLine;
    }
}
