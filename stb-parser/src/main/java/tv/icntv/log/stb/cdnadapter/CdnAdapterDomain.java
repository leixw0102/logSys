package tv.icntv.log.stb.cdnadapter;/*
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
 * Date: 2014/07/21
 * Time: 10:27
 */
public class CdnAdapterDomain {
    private String id;
    private String url;
    private String openTimeCost;
    private String seekCount;
    private String seekTimeAverTimeCost;
    private String playTotalTimeCost;
    private String buffCount;
    private String buffAverTimeCost;    //ms
    private String error;
    private String icntvId;

    private String date;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getBuffAverTimeCost() {
        return buffAverTimeCost;
    }

    public void setBuffAverTimeCost(String buffAverTimeCost) {
        this.buffAverTimeCost = buffAverTimeCost;
    }

    public String getBuffCount() {
        return buffCount;
    }

    public void setBuffCount(String buffCount) {
        this.buffCount = buffCount;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getIcntvId() {
        return icntvId;
    }

    public void setIcntvId(String icntvId) {
        this.icntvId = icntvId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOpenTimeCost() {
        return openTimeCost;
    }

    public void setOpenTimeCost(String openTimeCost) {
        this.openTimeCost = openTimeCost;
    }

    public String getPlayTotalTimeCost() {
        return playTotalTimeCost;
    }

    public void setPlayTotalTimeCost(String playTotalTimeCost) {
        this.playTotalTimeCost = playTotalTimeCost;
    }

    public String getSeekCount() {
        return seekCount;
    }

    public void setSeekCount(String seekCount) {
        this.seekCount = seekCount;
    }

    public String getSeekTimeAverTimeCost() {
        return seekTimeAverTimeCost;
    }

    public void setSeekTimeAverTimeCost(String seekTimeAverTimeCost) {
        this.seekTimeAverTimeCost = seekTimeAverTimeCost;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.getIcntvId()).append("|")
                .append(this.getDate()).append("|")
                .append(this.getId()).append("|")
                .append(this.getUrl()).append("|")
                .append(this.getOpenTimeCost()).append("|")
                .append(this.getBuffCount()).append("|")
                .append(this.getBuffAverTimeCost()).append("|")
                .append(this.getSeekCount()).append("|")
                .append(this.getSeekTimeAverTimeCost()).append("|")
                .append(this.getPlayTotalTimeCost()).append("|")
                .append(this.getError());
        return sb.toString();
    }
}
