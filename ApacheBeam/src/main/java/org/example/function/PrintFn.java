package org.example.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintFn extends DoFn<Object, Void> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final String prefix;
    public PrintFn() {
        this.prefix = "element";
    }
    public PrintFn(String prefix) {
        this.prefix = prefix;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        try {
            LOG.info(prefix + ": {}", c.element());
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}