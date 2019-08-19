/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.siddhi.extension.store.util;

import io.siddhi.query.api.definition.TableDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GCSConfig {
    private String authFilePath;
    private String bucketName;
    private String storageClass;
    private boolean setVersioningEnabled;
    private String contentType;
    private String objectName;
    private Map<String, String> bucketAclMap = new HashMap<>();

    public String getAuthFilePath() {
        return authFilePath;
    }

    public void setAuthFilePath(String authFilePath) {
        this.authFilePath = authFilePath;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public boolean isSetVersioningEnabled() {
        return setVersioningEnabled;
    }

    public void setSetVersioningEnabled(boolean setVersioningEnabled) {
        this.setVersioningEnabled = setVersioningEnabled;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public void setBucketACLMap(String bucketACLMap) {

        Matcher matcher = Pattern.compile("[a-zA-Z.@0-9]+:[a-zA-Z]+").matcher(bucketACLMap);

        while (matcher.find()) {
            this.bucketAclMap.put(matcher.group().split(":")[0], matcher.group().split(":")[1]);
        }

    }
}
