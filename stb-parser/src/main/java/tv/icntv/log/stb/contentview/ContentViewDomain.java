package tv.icntv.log.stb.contentview;/*
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

import com.google.common.base.Strings;
import tv.icntv.log.stb.commons.DateUtils;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/21
 * Time: 14:03
 */
public class ContentViewDomain {
    private String deviceCode;//icntvId
    private String operateType="";
    private String operateTime="";
    private String serviceType="1";
    private String videoType="";
    private String endReason="";
    private String chargeType="";
    private String catgId="";
    private String startDate="";
    private String endDate="";
    private String programId="";
    private String outerCode="";//节目集ID
    private String subjectID="";
    private String epgCode="06";
    private String std_path="";
    private String vodPathType="";
    private String dataSource="1";
    private String fSource="1";
    private String resolution="";
    private String mark1="";
    private String mark2="";

    public String getCatgId() {
        return catgId;
    }

    public void setCatgId(String catgId) {
        this.catgId = catgId;
    }

    public String getChargeType() {
        return chargeType;
    }

    public void setChargeType(String chargeType) {
        this.chargeType = chargeType;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getDeviceCode() {
        return deviceCode;
    }

    public void setDeviceCode(String deviceCode) {
        this.deviceCode = deviceCode;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getEndReason() {
        return endReason;
    }

    public void setEndReason(String endReason) {
        this.endReason = endReason;
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

    public String getMark1() {
        return mark1;
    }

    public void setMark1(String mark1) {
        this.mark1 = mark1;
    }

    public String getMark2() {
        return mark2;
    }

    public void setMark2(String mark2) {
        this.mark2 = mark2;
    }

    public String getOperateTime() {

        return operateTime;
    }

    public void setOperateTime(String operateTime) {
        this.operateTime = operateTime;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getOuterCode() {
        return outerCode;
    }

    public void setOuterCode(String outerCode) {
        this.outerCode = outerCode;
    }

    public String getProgramId() {
        return programId;
    }

    public void setProgramId(String programId) {
        this.programId = programId;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getStd_path() {
        return std_path;
    }

    public void setStd_path(String std_path) {
        this.std_path = std_path;
    }

    public String getSubjectID() {
        return subjectID;
    }

    public void setSubjectID(String subjectID) {
        this.subjectID = subjectID;
    }

    public String getVideoType() {
        return videoType;
    }

    public void setVideoType(String videoType) {
        this.videoType = videoType;
    }

    public String getVodPathType() {
        return vodPathType;
    }

    public void setVodPathType(String vodPathType) {
        this.vodPathType = vodPathType;
    }

    @Override
    public String toString() {
        if(!Strings.isNullOrEmpty(this.getStartDate())){
            this.setOperateType("1");
            this.setOperateTime(this.getStartDate());
        }
        if(!Strings.isNullOrEmpty(this.getEndDate())){
            this.setOperateType("2");
            this.setOperateTime(this.getEndDate());
        }

        StringBuffer sb = new StringBuffer();
        sb.append(this.getDeviceCode()).append("|")
                .append(this.getOperateType()).append("|")
                .append(DateUtils.getFormatDate(this.getOperateTime())).append("|")
                .append(this.getServiceType()).append("|")
                .append(this.getVideoType()).append("|")
                .append(this.getEndReason()).append("|")
                .append(this.getChargeType()).append("|")
                .append(this.getCatgId()).append("|")
                .append(this.getOuterCode()).append("|")
                .append(this.getProgramId()).append("|")
                .append(this.getSubjectID()).append("|")
                .append(this.getEpgCode()).append("|")
                .append(this.getStd_path()).append("|")
                .append(this.getVodPathType()).append("|")
                .append(this.getDataSource()).append("|")
                .append(this.getfSource()).append("|")
                .append(this.getResolution()).append("|")
                .append(this.getMark1()).append("|")
                .append(this.getMark2());
        return sb.toString();
    }

    @Override
    protected ContentViewDomain clone() {
        try {
            return (ContentViewDomain) super.clone();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (CloneNotSupportedException e) {
            return new ContentViewDomain();
        }
    }
}
