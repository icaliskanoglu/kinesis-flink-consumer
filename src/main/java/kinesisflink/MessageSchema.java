package kinesisflink;

import com.fasterxml.jackson.databind.*;
import org.apache.flink.api.common.serialization.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.*;
import org.apache.logging.log4j.*;

import java.io.*;

public class MessageSchema implements DeserializationSchema<Message> {
    private static final Logger LOGGER = LogManager.getLogger(MessageSchema.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public Message deserialize(byte[] bytes) throws IOException {
        Message criticalError = MAPPER.readValue(bytes, Message.class);
        return criticalError;
    }

    public boolean isEndOfStream(Message nextElement) {
        return false;
    }

    public TypeInformation<Message> getProducedType() {
        return TypeExtractor.getForClass(Message.class);
    }

}
