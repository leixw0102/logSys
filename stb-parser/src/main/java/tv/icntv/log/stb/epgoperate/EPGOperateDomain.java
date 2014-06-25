/*
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
package tv.icntv.log.stb.epgoperate;

import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.StringsUtils;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/06/20
 * Time: 11:31
 */
public class EPGOperateDomain {

	/**
	 * 用户序列号
	 */
	private String cntvid;
	/**
	 * EPG页面ID
	 */
	private String epgPageId;
	/**
	 * 源对象类型：1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
	 */
	private String sourceObjectType;
	/**
	 * 源对象ID
	 */
	private String sourceObjectId;
	/**
	 * 目标对象类型:1. EPG页面,2. 节目集,3. 节目,4. 栏目,5. 页面节点 (推荐位或列表位)
	 */
	private String targetObjectType;
	/**
	 * 目标对象ID
	 */
	private String targetObjectId;
	/**
	 * 搜索关键词:当操作类型为搜索时该字段需要传值
	 */
	private String Keyword;
	/**
	 * 操作类型标识
	 1-添加收藏
	 2-添加书签
	 11-跳转进入
	 12-跳转返回(保留)
	 21-搜索
	 22-用户注册
	 23-在线帮助
	 24-专题
	 25-播放
	 30-退出
	 31-异常退出
	 */
	private String OperType;
	/**
	 * 操作时间。格式是：YYYYMMDD HH24MISS
	 */
	private String opTime;
	/**
	 * 系统来源 1：易视腾,2：云立方,3：小米
	 */
	private String DataSource;
	/**
	 * 数据来源，见数据来源表
	 */
	private String Fsource;
	/**
	 * EPG版本编号,见EPGCode版本编号表
	 */
	private String EPGCode;
	/**
	 * 遥控设备类型 1. 遥控器,2. 多屏设备(若不能取到该数据则为空)
	 */
	private String RemoteControl;

	@Override
	public String toString() {

		StringBuffer sb=new StringBuffer();
		return sb.append(StringsUtils.getEncodeingStr(this.getCntvid())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getEpgPageId())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getSourceObjectType())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getSourceObjectId())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getTargetObjectType())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getTargetObjectId())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getKeyword())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getOperType())).append("|")
				.append(StringsUtils.getEncodeingStr(DateUtils.getFormatDate(this.getOpTime()))).append("|")
				.append(StringsUtils.getEncodeingStr(this.getDataSource())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getFsource())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getEPGCode())).append("|")
				.append(StringsUtils.getEncodeingStr(this.getRemoteControl())).toString();
	}

	public String getCntvid() {
		return cntvid;
	}

	public void setCntvid(String cntvid) {
		this.cntvid = cntvid;
	}

	public String getEpgPageId() {
		return epgPageId;
	}

	public void setEpgPageId(String epgPageId) {
		this.epgPageId = epgPageId;
	}

	public String getSourceObjectType() {
		return sourceObjectType;
	}

	public void setSourceObjectType(String sourceObjectType) {
		this.sourceObjectType = sourceObjectType;
	}

	public String getSourceObjectId() {
		return sourceObjectId;
	}

	public void setSourceObjectId(String sourceObjectId) {
		this.sourceObjectId = sourceObjectId;
	}

	public String getTargetObjectType() {
		return targetObjectType;
	}

	public void setTargetObjectType(String targetObjectType) {
		this.targetObjectType = targetObjectType;
	}

	public String getTargetObjectId() {
		return targetObjectId;
	}

	public void setTargetObjectId(String targetObjectId) {
		this.targetObjectId = targetObjectId;
	}

	public String getKeyword() {
		return Keyword;
	}

	public void setKeyword(String keyword) {
		Keyword = keyword;
	}

	public String getOperType() {
		return OperType;
	}

	public void setOperType(String operType) {
		OperType = operType;
	}

	public String getOpTime() {
		return opTime;
	}

	public void setOpTime(String opTime) {
		this.opTime = opTime;
	}

	public String getDataSource() {
		return DataSource;
	}

	public void setDataSource(String dataSource) {
		DataSource = dataSource;
	}

	public String getFsource() {
		return Fsource;
	}

	public void setFsource(String fsource) {
		Fsource = fsource;
	}

	public String getEPGCode() {
		return EPGCode;
	}

	public void setEPGCode(String EPGCode) {
		this.EPGCode = EPGCode;
	}

	public String getRemoteControl() {
		return RemoteControl;
	}

	public void setRemoteControl(String remoteControl) {
		RemoteControl = remoteControl;
	}
}
