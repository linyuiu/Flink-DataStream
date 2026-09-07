package org.linyureal.mock.writer;

import org.linyureal.model.cdc.Change;
import java.util.List;

public interface TradeWriter extends AutoCloseable {
    void write(List<Change> transaction) throws Exception;
    @Override void close() throws Exception;
}
