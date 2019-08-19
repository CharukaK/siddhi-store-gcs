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

public class GCSConstants {

    // Common attributes that are required for the publisher and reader
    public static final String AUTH_FILE_LOCATION_ATTRIBUTE_NAME = "auth.file.path";
    public static final String BUCKET_NAME_ATTRIBUTE_NAME = "bucket.name";

    // Attributes used by the publisher
    public static final String STORAGE_CLASS_ATTRIBUTE_NAME = "storage.class";
    public static final String SET_VERSIONING_ENABLED_ATTRIBUTE_NAME = "set.versioning.enabled";
    public static final String CONTENT_TYPE_ATTRIBUTE_NAME = "content.type";
    public static final String OBJECT_NAME_ATTRIBUTE_NAME = "object.name";
    public static final String BUCKET_ACL_ATTRIBUTE_NAME = "bucket.acl";
    public static final String OBJECT_ACL_ATTRIBUTE_NAME = "object.acl";
    public static final String OBJECT_METADATA_NAME = "object.metadata";

    public static final String BOOLEAN_STRING_VALUE_FALSE = "false";

}
