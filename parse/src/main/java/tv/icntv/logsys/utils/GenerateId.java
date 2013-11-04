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

package tv.icntv.logsys.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.sun.istack.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-10-29
 * Time: 下午1:47
 *
 */
public class GenerateId {
    private static AtomicLong atomicLong=new AtomicLong(0);
    private static Lock lock=new ReentrantLock(true);

    /**
     * 默认15位序列号
     * @return
     */
    public static String generateIdSuffix(){
       return generateIdSuffix(15);
    }

    private static String generateIdSuffix(int size) {
        try {
            lock.lock();
            long temp= atomicLong.getAndIncrement();
            if(temp== Long.MAX_VALUE){
                atomicLong.set(0);
                temp=atomicLong.getAndIncrement();
            }
            int caseInt=(temp+"").length();
            return getZero(size-caseInt)+temp;
        } finally {
            lock.unlock();
        }
    }

    private static String getZero(int length){
        StringBuffer sb =new StringBuffer(length);
        for(int i=0;i<length;i++){
            sb.append(0);
        }
        return sb.toString();
    }

    public static void main(String[]args){
        StringBuffer sb =new StringBuffer(12);
        for(int i=0;i<12;i++){
            sb.append(0);
        }
        System.out.println(sb.toString());
    }
}
