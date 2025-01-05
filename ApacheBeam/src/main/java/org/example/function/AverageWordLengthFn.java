package org.example.function;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import java.util.Objects;
import java.io.Serializable;

// Create subclass of CombineFn that has an accumulation type distinct from the input/output type
public class AverageWordLengthFn extends CombineFn<String, AverageWordLengthFn.Accumulator, Double> {
    public static class Accumulator implements Serializable {
        int sum = 0;
        int count = 0;
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Accumulator accumulator = (Accumulator) o;
            return sum == accumulator.sum && count == accumulator.count;
        }
        @Override
        public int hashCode() {
            return Objects.hash(sum, count);
        }
    }
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator addInput(Accumulator accumulator, String input) {
        if (accumulator == null) {
            return null;
        }
        accumulator.sum += (input != null ? input.length() : 0);
        accumulator.count++;
        return accumulator;
    }
    @Override
    public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
        Accumulator merged = createAccumulator();
        for (Accumulator accumulator : accumulators) {
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
        return merged;
    }
    @Override
    public Double extractOutput(Accumulator accumulator) {
        if (accumulator == null) {
            return null;
        }
        return (double) accumulator.sum / accumulator.count;
    }
}
