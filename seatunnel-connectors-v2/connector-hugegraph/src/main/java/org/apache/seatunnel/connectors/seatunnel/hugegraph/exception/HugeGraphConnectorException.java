package org.apache.seatunnel.connectors.seatunnel.hugegraph.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class HugeGraphConnectorException extends SeaTunnelRuntimeException {
    public HugeGraphConnectorException(SeaTunnelErrorCode code, Throwable c) {
        super(code, c);
    }

    public HugeGraphConnectorException(SeaTunnelErrorCode code, String msg) {
        super(code, msg);
    }

    public HugeGraphConnectorException(SeaTunnelErrorCode code, String msg, Throwable c) {
        super(code, msg, c);
    }
}
