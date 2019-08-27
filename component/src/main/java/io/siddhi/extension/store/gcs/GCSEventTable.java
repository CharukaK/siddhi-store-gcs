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

package io.siddhi.extension.store.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.gcs.beans.GCSConfig;
import io.siddhi.extension.store.gcs.exceptions.GCSTableException;
import io.siddhi.extension.store.gcs.util.GCSConstants;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.OptionalInt;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * This is a sample class-level comment, explaining what the Sink extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //AbstractRecordTable configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type =  "Supported parameter types.
 *                                        eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                          according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type =   "Supported parameter types.
 *                                         eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "false
 *                                         (if parameter doesn't depend on each event then dynamic parameter is false.
 *                                         In Source, only use static parameter)",
 *                               optional= "true/false, defaultValue= if it is optional then assign a default value
 *                                         according to the type."),
 * },
 * //If AbstractRecordTable system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first  system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "the default value of the system parameter.",
 *                                      possibleParameter="the possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax = "sample query that explain how extension use in Siddhi."
 *                              description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */

@Extension(
        name = "google-cloud-storage",
        namespace = "store",
        description = " ",
        parameters = {
                @Parameter(
                        name = GCSConstants.AUTH_FILE_LOCATION_ATTRIBUTE_NAME,
                        description = "Absolute path for the location of the auth file that is generated through the " +
                                "GCP console",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = GCSConstants.BUCKET_NAME_ATTRIBUTE_NAME,
                        description = "Name of the Bucket",
                        type = DataType.STRING
                ),
                @Parameter(
                        name = GCSConstants.STORAGE_CLASS_ATTRIBUTE_NAME,
                        type = DataType.STRING,
                        description = "Storage class of the storing objects"
                ),
                @Parameter(
                        name = GCSConstants.SET_VERSIONING_ENABLED_ATTRIBUTE_NAME,
                        type = DataType.BOOL,
                        description = "Enable versioning for the objects written to this bucket",
                        optional = true,
                        defaultValue = GCSConstants.BOOLEAN_STRING_VALUE_FALSE
                ),
                @Parameter(
                        name = GCSConstants.CONTENT_TYPE_ATTRIBUTE_NAME,
                        type = DataType.STRING,
                        description = "Content type of the Objects which are written to the bucket",
                        optional = true,
                        defaultValue = GCSConstants.DEFAULT_CONTENT_TYPE_VALUE
                ),
                @Parameter(
                        name = GCSConstants.BUCKET_ACL_ATTRIBUTE_NAME,
                        type = DataType.STRING,
                        description = "ACL for bucket level as a Map",
                        optional = true,
                        defaultValue = "null"
                ),
                @Parameter(
                        name = GCSConstants.OBJECT_ACL_ATTRIBUTE_NAME,
                        type = DataType.STRING,
                        description = "Object level ACL map",
                        optional = true,
                        defaultValue = "null"
                ),
                @Parameter(
                        name = GCSConstants.OBJECT_METADATA_NAME,
                        type = DataType.STRING,
                        description = "Custom metadata defined by the user",
                        optional = true,
                        defaultValue = "null"
                )
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/#table

public class GCSEventTable extends AbstractRecordTable {

    private List<Attribute> attributes;
    private Storage storage;
    private GCSConfig config;
    private Annotation primaryKeyAnnotation;

    private static final Logger logger = Logger.getLogger(GCSEventTable.class);


    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractRecordTable}:wq configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();
        this.primaryKeyAnnotation = AnnotationHelper.getAnnotation(
                SiddhiConstants.ANNOTATION_PRIMARY_KEY, tableDefinition.getAnnotations());
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(
                SiddhiConstants.ANNOTATION_STORE, tableDefinition.getAnnotations());

        config = new GCSConfig(storeAnnotation);
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        createObject(records);
    }

    /**
     * Create an object in the specified Google Cloud Storage bucket.
     * @param records List of record object arrays.
     */
    private void createObject(List<Object[]> records) {
        createBucketIfNotExists();

        // create an object for each event received.
        records.forEach(payload -> {
            int primaryIndex = IntStream.range(0, attributes.size()).filter(i -> attributes.get(i).getName()
                    .equals(primaryKeyAnnotation.getElements().get(0).getValue().trim())).findFirst().getAsInt();

            OptionalInt metadataIndex = IntStream.range(0, attributes.size()).filter(i -> attributes.get(i).getName()
                    .equals(config.getMetadataField())).findFirst();

            OptionalInt objectAclIndex = IntStream.range(0, attributes.size()).filter(i -> attributes.get(i).getName()
                    .equals(config.getObjectAclField())).findFirst();

            BlobId blobId = BlobId.of(config.getBucketName(), payload[primaryIndex].toString());
            BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId).setContentType(config.getContentType());

            if (metadataIndex.isPresent()) {
                blobInfoBuilder.setMetadata((HashMap<String, String>) payload[metadataIndex.getAsInt()]);
            }

            if (objectAclIndex.isPresent()) {
                blobInfoBuilder.setAcl(
                        getObjectLevelAclList((HashMap<String, String>) payload[objectAclIndex.getAsInt()]));
            }

            StringBuilder payloadString = new StringBuilder();

            for (int i = 0; i < payload.length; i++) {
                if (i != primaryIndex && (metadataIndex.isPresent() &&
                        metadataIndex.getAsInt() != i) && (objectAclIndex.isPresent() &&
                        objectAclIndex.getAsInt() != i)) {

                    if (payloadString.length() == 0) {
                        payloadString.append(payload[i].toString());
                    } else {
                        payloadString.append(String.format(",'%s'", payload[i]));
                    }

                }
            }

            storage.create(blobInfoBuilder.build(), payloadString.toString().getBytes(StandardCharsets.UTF_8));
        });
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */
    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        System.out.println("shshsh");
        return null;
    }

    /**
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        return false;
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        for (Map<String, Object> conditionParams : deleteConditionParameterMaps) {
            BlobId blobId = BlobId.of(config.getBucketName(),
                    conditionParams.get(((GCSCompiledCondition) compiledCondition)
                            .getCompiledQuery().getStreamVariable().getName()).toString());

            boolean deleted = storage.delete(blobId);

            if (deleted) {
                logger.info(String.format("Object with the name %s is deleted from the bucket %s.",
                        conditionParams.get(((GCSCompiledCondition) compiledCondition)
                                .getCompiledQuery().getStreamVariable().getName()).toString(),
                                                                    config.getBucketName()));
            } else {
                logger.warn(String.format("Object with the name %s could not be found in the bucket %s.",
                        conditionParams.get(((GCSCompiledCondition) compiledCondition)
                                .getCompiledQuery().getStreamVariable().getName()).toString(),
                                                                        config.getBucketName()));
            }
        }
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {
        System.out.println("update");

        for (Map<String, Object> keyValuePair: list) {


        }

    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the values for adding new records if the update condition did not match
     * @param list2             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {
        System.out.println("update Or Add");

//        ReadChannel readChannel = getObjectReadChannel(config.getBucketName(), );
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        GCSExpressionVisitor gcsExpressionVisitor = new GCSExpressionVisitor();
        expressionBuilder.build(gcsExpressionVisitor);

        if (gcsExpressionVisitor.getCompareOperation().isValid()) {
            return new GCSCompiledCondition(gcsExpressionVisitor.getCompareOperation());
        } else {
            throw new GCSTableException("Invalid BasicCompareOperation object received from the expression visitor");
        }
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {
        try {
            storage = StorageOptions.newBuilder()
                    .setCredentials(
                            GoogleCredentials.fromStream(new FileInputStream(
                                    new File(config.getAuthFilePath())))).build().getService();
        } catch (IOException e) {
            logger.error("Error occurred while trying to connect to the Google Cloud Storage", e);
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect.
     */
    @Override
    protected void disconnect() {
        storage = null;
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {

    }

    private void createBucketIfNotExists() {
        // Check if the bucket exists in the GCS

        if (storage != null && storage.get(config.getBucketName(), Storage.BucketGetOption.fields()) == null) {

            //Create a bucket when it is not existing
            storage.create(BucketInfo.newBuilder(config.getBucketName())
                    .setStorageClass(config.getStorageClass())
                    .setVersioningEnabled(config.isVersioningEnabled()).build());

            // Set user defined ACLs for the bucket
            for (Map.Entry<String, String> item : config.getBucketACLMap().entrySet()) {
                switch (item.getValue().toLowerCase()) {
                    case "owner":
                        createAclForUser(item.getKey(), Acl.Role.OWNER);
                        break;
                    case "reader":
                        createAclForUser(item.getKey(), Acl.Role.READER);
                        break;
                    case "writer":
                        createAclForUser(item.getKey(), Acl.Role.WRITER);
                        break;
                    default:
                        // not a valid type of Permission.
                }
            }


        }
    }

    private Acl createAclForUser(String email, Acl.Role role) {
        return storage.createAcl(config.getBucketName(), Acl.of(new Acl.User(email), role));
    }

    private List<Acl> getObjectLevelAclList(HashMap<String, String> aclMap) {
        List<Acl> objectAclList = new ArrayList<>();

        for (Map.Entry<String, String> entry : aclMap.entrySet()) {
            Acl acl = null;

            switch (entry.getValue().toLowerCase()) {
                case GCSConstants.USER_TYPE_OWNER:
                    acl = Acl.of(new Acl.User(entry.getKey()), Acl.Role.OWNER);
                    break;
                case GCSConstants.USER_TYPE_READER:
                    acl = Acl.of(new Acl.User(entry.getKey()), Acl.Role.READER);
                    break;
                case GCSConstants.USER_TYPE_WRITER:
                    acl = Acl.of(new Acl.User(entry.getKey()), Acl.Role.WRITER);
                    break;
                default:
                    // not an valid Acl entry
                    return Collections.emptyList();
            }

            objectAclList.add(acl);
        }

        return objectAclList;
    }


    private ReadChannel getObjectReadChannel(String bucketName, String objectName) {
        return storage.get(bucketName, objectName).reader();
    }

}
