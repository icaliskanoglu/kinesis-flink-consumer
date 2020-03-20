package kinesisflink;

public class Message {
    public String message;
    public long time;
    public String id;
    public String alias;
    public int sum;

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", time=" + time +
                ", id='" + id + '\'' +
                ", alias='" + alias + '\'' +
                ", sum='" + sum + '\'' +
                '}';
    }
}