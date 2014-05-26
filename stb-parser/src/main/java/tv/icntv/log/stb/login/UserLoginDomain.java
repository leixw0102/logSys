package tv.icntv.log.stb.login;/*
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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.StringsUtils;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/23
 * Time: 17:47
 */
public class UserLoginDomain {
    private String icntvId="";
    private String operateType="";
    private String operateTime="";
    private String ipAddress="";
    private String dataSource="1";
    private String fsource="1";
    private String remoteControl="";

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getFsource() {
        return fsource;
    }

    public void setFsource(String fsource) {
        this.fsource = fsource;
    }

    public String getIcntvId() {
        return icntvId;
    }

    public void setIcntvId(String icntvId) {
        this.icntvId = icntvId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {

        this.ipAddress = ipAddress;
    }

    public String getOperateTime() {
        return operateTime;
    }

    public void setOperateTime(String operateTime) {
        this.operateTime = operateTime;
    }
    String OP_TYPE_STARTUP = "1";
    String OP_TYPE_SHUTDOWN = "2";
    String OP_TYPE_ACTIVATE = "3";
    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        if("STARTUP".equalsIgnoreCase(operateType)){
            this.operateType=OP_TYPE_STARTUP;
        }else if("SHUTDOWN".equalsIgnoreCase(operateType)){
            this.operateType=OP_TYPE_SHUTDOWN;
        }else if("ACTIVATE".equalsIgnoreCase(operateType)){
            this.operateType=OP_TYPE_ACTIVATE;
        }else{

        }

    }

    public String getRemoteControl() {
        return remoteControl;
    }

    public void setRemoteControl(String remoteControl) {
        this.remoteControl = remoteControl;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        return sb.append(StringsUtils.getEncodeingStr(this.getIcntvId()))
                .append("|")
                .append(StringsUtils.getEncodeingStr(this.getOperateType()))
                .append("|")
                .append(StringsUtils.getEncodeingStr(DateUtils.getFormatDate(this.getOperateTime())))
                .append("|")
                .append(StringsUtils.getEncodeingStr(this.getIpAddress()))
                .append("|")
                .append(StringsUtils.getEncodeingStr(this.getDataSource()))
                .append("|")
                .append(StringsUtils.getEncodeingStr(this.getFsource()))
                .append("|")
                .append(StringsUtils.getEncodeingStr(this.getRemoteControl())).toString();
    }
    public static void main(String[]args){
        UserLoginDomain loginDomain=new UserLoginDomain();
        System.out.println(loginDomain.toString());
    }
}
