package kinesisflink;

import org.apache.flink.streaming.api.functions.sink.*;
import org.apache.logging.log4j.*;

public class MessageSink extends RichSinkFunction<Message> {
    private static final Logger LOGGER = LogManager.getLogger(MessageSink.class);

    public void invoke(Message record, Context context) {
        StringBuilder sb = new StringBuilder();
        sb.append("timestamp: ").append(context.timestamp());
        sb.append(" sum: ").append(record.sum);
        sb.append(" alias: ").append(record.alias);
        sb.append(" profile: ").append(record.message);
        sb.append(" logTime: ").append(record.time);
        System.out.println(sb.toString());
    }
}
