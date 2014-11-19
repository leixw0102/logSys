package tv.icntv.log.stb.cdnModule;/*
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

import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/23
 * Time: 09:23
 */
public class CdnStbDomain {
    private String cntvId="";
    private String host="";
    private String url="";
    private String programId="";
    private String resolution="";
    private String startTime="";
    private String endTime="";
    private String mac;

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    private String id="", userIp="",taskCnt="",sucCnt="",failCnt="",conFailCnt="",timeOutCnt="",nooFileErrorCnt="",srvCloseCnt="",srvErrorCnt="",socketErrorCnt="",revByte="",revSpeed="",dnsAvgTime="",dnsMaxTime="",dnsMinTime="",conAvgTime="",conMaxTime="",conMinTime="",dnsRedList="";

    public String getProgramId() {
        return programId;
    }

    public void setProgramId(String programId) {
        this.programId = programId;
    }

    private String mark1="";
    protected void getResolutionType(){
        if(this.getUrl().contains("HD1M")){
             this.setResolution("1");
        }else if (this.getUrl().contains("HD2M")){
            this.setResolution("3");
        } else if( this.getUrl().contains("SD")){
            this.setResolution("2");
        }
        this.setResolution("99");
    }

    public String getCntvId() {
        return cntvId;
    }

    public void setCntvId(String cntvId) {
        this.cntvId = cntvId;
    }


    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMark1() {
        return mark1;
    }

    public void setMark1(String mark1) {
        this.mark1 = mark1;
    }

    public String getUserIp() {
        return userIp;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getConAvgTime() {
        return conAvgTime;
    }

    public void setConAvgTime(String conAvgTime) {
        this.conAvgTime = conAvgTime;
    }

    public String getConFailCnt() {
        return conFailCnt;
    }

    public void setConFailCnt(String conFailCnt) {
        this.conFailCnt = conFailCnt;
    }

    public String getConMaxTime() {
        return conMaxTime;
    }

    public void setConMaxTime(String conMaxTime) {
        this.conMaxTime = conMaxTime;
    }

    public String getConMinTime() {
        return conMinTime;
    }

    public void setConMinTime(String conMinTime) {
        this.conMinTime = conMinTime;
    }

    public String getDnsAvgTime() {
        return dnsAvgTime;
    }

    public void setDnsAvgTime(String dnsAvgTime) {
        this.dnsAvgTime = dnsAvgTime;
    }

    public String getDnsMaxTime() {
        return dnsMaxTime;
    }

    public void setDnsMaxTime(String dnsMaxTime) {
        this.dnsMaxTime = dnsMaxTime;
    }

    public String getDnsMinTime() {
        return dnsMinTime;
    }

    public void setDnsMinTime(String dnsMinTime) {
        this.dnsMinTime = dnsMinTime;
    }

    public String getDnsRedList() {
        return dnsRedList;
    }

    public void setDnsRedList(String dnsRedList) {
        this.dnsRedList = dnsRedList;
    }

    public String getFailCnt() {
        return failCnt;
    }

    public void setFailCnt(String failCnt) {
        this.failCnt = failCnt;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getNooFileErrorCnt() {
        return nooFileErrorCnt;
    }

    public void setNooFileErrorCnt(String nooFileErrorCnt) {
        this.nooFileErrorCnt = nooFileErrorCnt;
    }

    public String getResolution() {
        return resolution;
    }

    public String getRevByte() {
        return revByte;
    }

    public void setRevByte(String revByte) {
        this.revByte = revByte;
    }

    public String getRevSpeed() {
        return revSpeed;
    }

    public void setRevSpeed(String revSpeed) {
        this.revSpeed = revSpeed;
    }

    public String getSocketErrorCnt() {
        return socketErrorCnt;
    }

    public void setSocketErrorCnt(String socketErrorCnt) {
        this.socketErrorCnt = socketErrorCnt;
    }

    public String getSrvCloseCnt() {
        return srvCloseCnt;
    }

    public void setSrvCloseCnt(String srvCloseCnt) {
        this.srvCloseCnt = srvCloseCnt;
    }

    public String getSrvErrorCnt() {
        return srvErrorCnt;
    }

    public void setSrvErrorCnt(String srvErrorCnt) {
        this.srvErrorCnt = srvErrorCnt;
    }

    public String getSucCnt() {
        return sucCnt;
    }

    public void setSucCnt(String sucCnt) {
        this.sucCnt = sucCnt;
    }

    public String getTaskCnt() {
        return taskCnt;
    }

    public void setTaskCnt(String taskCnt) {
        this.taskCnt = taskCnt;
    }

    public String getTimeOutCnt() {
        return timeOutCnt;
    }

    public void setTimeOutCnt(String timeOutCnt) {
        this.timeOutCnt = timeOutCnt;
    }

    @Override
    public String toString() {
        try{
        this.getResolutionType();
        StringBuffer sb = new StringBuffer();
        sb.append(this.getId()).append("|")
                .append(this.getCntvId()).append("|")
                .append(this.getMac()).append("|")
                .append(this.getUserIp()).append("|")
                .append(this.getUrl()).append("|")
                .append(this.getProgramId()).append("|")
                .append(this.getResolution()).append("|")
                .append(this.getStartTime()).append("|")
                .append(this.getEndTime()).append("|")
                .append(this.getHost()).append("|")
                .append(this.getTaskCnt()).append("|")
                .append(this.getSucCnt()).append("|")
                .append(this.getFailCnt()).append("|")
                .append(this.getConFailCnt()).append("|")
                .append(this.getTimeOutCnt()).append("|")
                .append(this.getNooFileErrorCnt()).append("|")
                .append(this.getSrvCloseCnt()).append("|")
                .append(this.getSrvErrorCnt()).append("|")
                .append(this.getSocketErrorCnt()).append("|")
                .append(this.getRevByte()).append("|")
                .append(this.getRevSpeed()).append("|")
                .append(this.getDnsAvgTime()).append("|")
                .append(this.getDnsMaxTime()).append("|")
                .append(this.getDnsMinTime()).append("|")
                .append(this.getConAvgTime()).append("|")
                .append(this.getConMaxTime()).append("|")
                .append(this.getConMinTime()).append("|")
                .append(this.getDnsRedList()).append("|");
//                .append(this.getMark1());
        return sb.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return "";
    }
}
