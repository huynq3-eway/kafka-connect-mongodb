/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.cdc.debezium;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.cdc.CdcHandler;
import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class DebeziumCdcHandler extends CdcHandler {

    public static final String OPERATION_TYPE_FIELD_PATH = "op";

    private static Logger logger = LoggerFactory.getLogger(DebeziumCdcHandler.class);

    private final Map<OperationType,CdcOperation> operations = new HashMap<>();

    public DebeziumCdcHandler(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    protected void registerOperations(Map<OperationType,CdcOperation> operations) {
        logger.debug("(registerOperations)operations: {}", operations);
        this.operations.putAll(operations);
    }

    public CdcOperation getCdcOperation(BsonDocument doc) {
        try {
            logger.debug("(getCdcOperation)operations: {}", operations);
            if(!doc.containsKey(OPERATION_TYPE_FIELD_PATH)
                    || !doc.get(OPERATION_TYPE_FIELD_PATH).isString()) {
                throw new DataException("error: value doc is missing CDC operation type of type string");
            }
            CdcOperation op = operations.get(OperationType.fromText(
                    doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue())
            );
            logger.debug("(getCdcOperation)op: {}", op);
            if(op == null) {
                throw new DataException("error: no CDC operation found in mapping for op="
                        + doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue());
            }
            return op;
        } catch (IllegalArgumentException exc){
            throw new DataException("error: parsing CDC operation failed",exc);
        }
    }

}
