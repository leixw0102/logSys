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

import tv.icntv.log.stb.commons.DateUtils;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 14:45
 */
public class CdnServer  {
    @Override
    protected CdnServer clone() {
        try {
            return (CdnServer) super.clone();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (CloneNotSupportedException e) {
            return new CdnServer();
        }
    }

    private String icntvId;
    private String userIp;
    private String ua;
    private String cdnFactory;
    private String domain;
    private String time;
    private String sliceSize;
    private String responseTime;
    private String mark1;
    private String mark2;

    public String getCdnFactory() {
        return cdnFactory;
    }

    public void setCdnFactory(String cdnFactory) {
        this.cdnFactory = cdnFactory;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getIcntvId() {
        return icntvId;
    }

    public void setIcntvId(String icntvId) {
        this.icntvId = icntvId;
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

    public String getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(String responseTime) {
        this.responseTime = responseTime;
    }

    public String getSliceSize() {
        return sliceSize;
    }

    public void setSliceSize(String sliceSize) {
        this.sliceSize = sliceSize;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public String getUserIp() {
        return userIp;
    }

    public void setUserIp(String userIp) {
        this.userIp = userIp;
    }

    /**
     * icntv+url+ip+sliceSize+status+time+ua
     * @param value
     */
    public void parser(String value){
        String[] vs=value.split("\\|");
        this.setIcntvId(vs[0]);
        this.setDomain(vs[1]);
        this.setUserIp(vs[2]);
        this.setSliceSize(vs[3]);
        this.setTime(DateUtils.getFormatDate(vs[5]));
        this.setUa(vs[6]);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getIcntvId()).append("|")
                .append(this.getUserIp()).append("|")
                .append(this.getUa()).append("|")
                .append(this.getCdnFactory()).append("|")
                .append(this.getDomain()).append("|")
                .append(this.getTime()).append("|")
                .append(this.getSliceSize()).append("|")
                .append(this.getResponseTime()).append("|")
                .append(this.getMark1()).append("|")
                .append(this.getMark2());
        return sb.toString();
    }
}
