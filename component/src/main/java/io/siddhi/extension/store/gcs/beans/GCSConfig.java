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

package io.siddhi.extension.store.gcs.beans;

import com.google.cloud.storage.StorageClass;
import io.siddhi.extension.store.gcs.util.GCSConstants;
import io.siddhi.query.api.annotation.Annotation;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Config class that contains all the configurations that is required for the GCS connector.
 */
public class GCSConfig {
    private String authFilePath;
    private String bucketName;
    private String storageClass;
    private boolean setVersioningEnabled;
    private String objectName;
    private String metadataField;
    private String objectAclField;
    private Map<String, String> bucketAclMap = new HashMap<>();
    private String contentType = GCSConstants.DEFAULT_CONTENT_TYPE_VALUE;

    public GCSConfig(Annotation storeAnnotation) {
        this.authFilePath = storeAnnotation.getElement(GCSConstants.AUTH_FILE_LOCATION_ATTRIBUTE_NAME);
        this.storageClass = storeAnnotation.getElement(GCSConstants.STORAGE_CLASS_ATTRIBUTE_NAME);
        this.bucketName = storeAnnotation.getElement(GCSConstants.BUCKET_NAME_ATTRIBUTE_NAME);

        if (storeAnnotation.getElement(GCSConstants.CONTENT_TYPE_ATTRIBUTE_NAME) != null) {
            this.contentType = storeAnnotation.getElement(GCSConstants.CONTENT_TYPE_ATTRIBUTE_NAME);
        }

        if (storeAnnotation.getElement(GCSConstants.SET_VERSIONING_ENABLED_ATTRIBUTE_NAME) != null) {
            this.setVersioningEnabled = Boolean.parseBoolean(
                    storeAnnotation.getElement(GCSConstants.SET_VERSIONING_ENABLED_ATTRIBUTE_NAME));
        }

        if (storeAnnotation.getElement(GCSConstants.BUCKET_ACL_ATTRIBUTE_NAME) != null) {
            setBucketACLMap(storeAnnotation.getElement(GCSConstants.BUCKET_ACL_ATTRIBUTE_NAME));
        }

        if (storeAnnotation.getElement(GCSConstants.OBJECT_METADATA_NAME) != null) {
            setMetadataField(storeAnnotation.getElement(GCSConstants.OBJECT_METADATA_NAME));
        }
    }

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

    public StorageClass getStorageClass() {
        switch (this.storageClass.toLowerCase()) {
            case "multi-regional":
                return StorageClass.MULTI_REGIONAL;
            case "regional":
                return StorageClass.REGIONAL;
            case "nearline":
                return StorageClass.NEARLINE;
            case "coldline":
                return StorageClass.COLDLINE;
            default:
                // not a supported version of StorageClass
        }

        return null;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    public boolean isVersioningEnabled() {
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

    public String getMetadataField() {
        return metadataField;
    }

    public void setMetadataField(String metadataField) {
        this.metadataField = metadataField;
    }

    public String getObjectAclField() {
        return objectAclField;
    }

    public void setObjectAclField(String objectAclField) {
        this.objectAclField = objectAclField;
    }

    public void setBucketACLMap(String bucketACLMap) {

        Matcher matcher = Pattern.compile("[a-zA-Z.@0-9]+:[a-zA-Z]+").matcher(bucketACLMap);

        while (matcher.find()) {
            this.bucketAclMap.put(matcher.group().split(":")[0], matcher.group().split(":")[1]);
        }

    }

    public Map<String, String> getBucketACLMap() {
        return bucketAclMap;
    }


}
