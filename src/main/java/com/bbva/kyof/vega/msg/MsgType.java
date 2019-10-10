package com.bbva.kyof.vega.msg;

/**
 * Class that contains all the message types the framework can send
 */
public final class MsgType
{
    /** User message type */
    public static final byte DATA = 0;

    /** User request  message type */
    public static final byte DATA_REQ = 1;

    /** Response message type, it can be a response to a user request or to an internal library request like a heartbeat */
    public static final byte RESP = 2;

    /** Auto discovery topic info message type */
    public static final byte AUTO_DISC_TOPIC = 3;

    /** Auto discovery topic socket info message type */
    public static final byte AUTO_DISC_TOPIC_SOCKET = 4;

    /** Auto discovery instance info message type */
    public static final byte AUTO_DISC_INSTANCE = 5;

    /** Auto discovery client resolver information */
    public static final byte AUTO_DISC_DAEMON_CLIENT_INFO = 6;

    /** Heartbeat request */
    public static final byte HEARTBEAT_REQ = 7;

    /** Control security request */
    public static final byte CONTROL_SECURITY_REQ = 8;

    /** Control security response */
    public static final byte CONTROL_SECURITY_RESP = 9;

    /** Control security error response */
    public static final byte CONTROL_SECURITY_ERROR_RESP = 10;

    /** Encrypted data message */
    public static final byte ENCRYPTED_DATA = 11;

    /** Auto discovery unicast server resolver information */
    public static final byte AUTO_DISC_DAEMON_SERVER_INFO = 12;

    /** Private constructor to avoid instantiation */
    private MsgType()
    {
        // Nothing to do
    }

    /**
     * Convert the message type to String
     * @param byteValue byte value of the message type
     * @return the String representation
     */
    public static String toString(final byte byteValue)
    {
        switch (byteValue)
        {
            case DATA: return "DATA";
            case DATA_REQ: return "DATA_REQ";
            case RESP: return "RESP";
            case AUTO_DISC_TOPIC: return "AUTO_DISC_TOPIC";
            case AUTO_DISC_TOPIC_SOCKET: return "AUTO_DISC_TOPIC_SOCKET";
            case AUTO_DISC_INSTANCE: return "AUTO_DISC_INSTANCE";
            case AUTO_DISC_DAEMON_CLIENT_INFO: return "AUTO_DISC_DAEMON_CLIENT_INFO";
            case HEARTBEAT_REQ: return "HEARTBEAT_REQ";
            case CONTROL_SECURITY_REQ: return "CONTROL_SECURITY_REQ";
            case CONTROL_SECURITY_RESP: return "CONTROL_SECURITY_RESP";
            case ENCRYPTED_DATA: return "ENCRYPTED_DATA";
            case CONTROL_SECURITY_ERROR_RESP: return  "CONTROL_SECURITY_ERROR_RESP";
            case AUTO_DISC_DAEMON_SERVER_INFO: return "AUTO_DISC_DAEMON_SERVER_INFO";
            default: return "UNKNOWN";
        }
    }
}
