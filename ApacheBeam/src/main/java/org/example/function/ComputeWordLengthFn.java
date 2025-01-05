package org.example.function;

import org.apache.beam.sdk.transforms.DoFn;

public class ComputeWordLengthFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(@Element String word, OutputReceiver<Integer> out) throws Exception {
        try {
            // Replace all in try clause with out.output(w.length()); if only count one word's length
            String[] words = word.split(" ");
            for (String w: words) {
                if (!w.isEmpty()) {
                    out.output(w.length());
                }
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}
