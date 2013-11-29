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

package tv.icntv.log.crawl.json;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-15
 * Time: 下午2:37
 */
public class DownFile {
    private String name;
    private String status;
    private long size;
    public String getName() {
        return name;
    }
    public String toJson(){
        return JSON.toJSONString(this);
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public static class DownFileBuilder{
        private String name;
        private String status;
        private long size;

        public String getName() {
            return name;
        }

        public DownFileBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public String getStatus() {
            return status;
        }

        public DownFileBuilder setStatus(String status) {
            this.status = status;
            return this;
        }

        public long getSize() {
            return size;
        }

        public DownFileBuilder setSize(long size) {
            this.size = size;
            return this;
        }
        public DownFile builder(){
            DownFile downFile=new DownFile();
            downFile.setName(this.getName());
            downFile.setSize(this.getSize());
            downFile.setStatus(this.getStatus());
            return downFile;
        }

    }

    public static  class  JsonToDownFileBuilder{
            private String json;

        public String getJson() {
            return json;
        }

        public  JsonToDownFileBuilder setJson(String json) {
            this.json = json;
            return this;
        }

        public DownFile builder(){
            Preconditions.checkNotNull(this.getJson(),"json string null");
            return JSON.parseObject(this.getJson(),DownFile.class);
        }
    }

    public static void main(String [] args){
        String json=new DownFileBuilder().setStatus("sss").setSize(22).builder().toJson();
        System.out.println(json);
        DownFile file = new JsonToDownFileBuilder().setJson(json).builder();
        System.out.println(file.getSize());
    }
}
