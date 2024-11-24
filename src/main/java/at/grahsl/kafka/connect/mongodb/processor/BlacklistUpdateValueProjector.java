package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.BlacklistProjector;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Predicate;

public class BlacklistUpdateValueProjector extends BlacklistProjector {

    private static Logger LOGGER = LoggerFactory.getLogger(BlacklistUpdateValueProjector.class);
    private Predicate<MongoDbSinkConnectorConfig> predicate;

    public BlacklistUpdateValueProjector(MongoDbSinkConnectorConfig config, String collection) {
        this(config,config.getUpdateValueProjectionList(collection),
                cfg -> cfg.isUsingBlacklistValueProjection(collection),collection);
    }

    public BlacklistUpdateValueProjector(MongoDbSinkConnectorConfig config, Set<String> fields,
                                         Predicate<MongoDbSinkConnectorConfig> predicate, String collection) {
        super(config,collection);
        this.fields = fields;
        this.predicate = predicate;
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {
        Header operatorHeader = orig.headers().lastWithName("__op");
        LOGGER.debug("(process)operatorHeader: {}", operatorHeader);
        if (operatorHeader != null && operatorHeader.value().toString().equalsIgnoreCase("u")) {
            LOGGER.debug("(process)is update!");
            if(predicate.test(getConfig())) {
                doc.getValueDoc().ifPresent(vd ->
                        fields.forEach(f -> doProjection(f,vd))
                );
            }
        }


        getNext().ifPresent(pp -> pp.process(doc,orig));
    }

}
