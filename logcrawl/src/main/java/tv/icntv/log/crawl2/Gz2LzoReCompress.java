package tv.icntv.log.crawl2;/*
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

import tv.icntv.log.crawl2.compression.Compress;
import tv.icntv.log.crawl2.decompression.UnCompress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/04/14
 * Time: 14:17
 */
public class Gz2LzoReCompress extends AbstractReCompress {
    private UnCompress unCompress;
    private Compress compress;
    public Gz2LzoReCompress(String sourceFile, String targetFile) {
        super(sourceFile, targetFile);
    }

    public Compress getCompress() {
        return compress;
    }

    public void setCompress(Compress compress) {
        this.compress = compress;
    }

    public UnCompress getUnCompress() {
        return unCompress;
    }

    public void setUnCompress(UnCompress unCompress) {
        this.unCompress = unCompress;
    }

    public Gz2LzoReCompress(String sourceFile, String targetFile, Compress compress, UnCompress unCompress) {
        super(sourceFile, targetFile);
        this.compress = compress;
        this.unCompress = unCompress;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return getUnCompress().getInputStream(getSourceFile());
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return getCompress().getOutputStream(getTargetFile());
    }
}
