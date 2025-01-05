package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.function.AverageWordLengthFn;
import org.example.function.ComputeWordLengthFn;
import org.example.function.PrintFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
This is the first part of examples rewrite for TourOfBeam https://tour.beam.apache.org/tour/java
Mainly covers common transforms and core transforms
*/
public class TourOfBeam {
    public static void filter(Pipeline pipeline, List<Integer> input) {
        pipeline
                .apply("CreateFilterInput", Create.of(input))
                // Return even numbers, deal with null exception
                .apply(Filter.by(number -> number != null ? number % 2 == 0 : null))
                .apply("FilterToEvenNumbers", ParDo.of(new PrintFn()))
        ;
    }
    public static void count(Pipeline pipeline, List<Integer> input) {
        /* For Integers
        Count / Mean: Count.globally()
        Sum / Min / Max: Sum.integersGlobally() / Sum.doublesGlobally()
        */
        pipeline
                .apply("CreateCountInput", Create.of(input))
                .apply(Count.globally())
                .apply("CountIntegers", ParDo.of(new PrintFn()))
        ;
    }
    public static void sum(Pipeline pipeline, List<KV<String, Integer>> inputKV) {
        /* For KV
        Count / Mean: Count.perKey()
        Sum / Min / Max: Sum.integersPerKey()
        */
        pipeline
                .apply("CreateSumInput", Create.of(inputKV))
                .apply(Sum.integersPerKey())
                .apply("SumKeyIntegers", ParDo.of(new PrintFn()))
        ;
    }
    public static void parDoOneToMany(Pipeline pipeline, String inputWord) {
        pipeline
                .apply("CreateWordInput", Create.of(inputWord))
                // Return length of each word after splitting with blank space
                .apply("ComputeWordLengths", ParDo.of(new ComputeWordLengthFn()))
                .apply("OutputWordLengths", ParDo.of(new PrintFn()))
        ;
    }
    public static void mapElements(Pipeline pipeline, List<String> inputString) {
        pipeline
                .apply("CreateStringInput", Create.of(inputString))
                // Return length of the word, deal with null exception
                .apply(MapElements.into(TypeDescriptors.integers())
                        .via((String word) -> word != null ? word.length() : 0))
                .apply("ComputeElementsLengths", ParDo.of(new PrintFn()))
        ;
    }
    public static void flatMapElements(Pipeline pipeline, List<KV<String, Integer>> inputKV) {
        pipeline
                .apply("CreateMapInput", Create.of(inputKV))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((KV<String, Integer> wordWithCount) -> {
                    List<String> words = new ArrayList<>();
                    if (wordWithCount != null && wordWithCount.getValue() != null) {
                        for (int i = 0; i < wordWithCount.getValue(); i++) {
                            words.add(wordWithCount.getKey());
                        }
                    }
                    return words;
                }))
                .apply("OutputMapKeyElements", ParDo.of(new PrintFn()))
        ;
    }
    public static void groupByKey(Pipeline pipeline, List<KV<String, Integer>> inputKV) {
        pipeline
                .apply("CreateGroupInput", Create.of(inputKV))
                .apply(GroupByKey.create())
                .apply("GroupValuesByKey", ParDo.of(new PrintFn()))
        ;
    }
    public static void coGroupByKey(Pipeline pipeline,
                                    List<KV<String, Integer>> inputKV,
                                    List<KV<String, String>> inputKVName) {
        KeyedPCollectionTuple
                .of("TupleInputKV", pipeline.apply("CreateCoGroupInput", Create.of(inputKV)))
                .and("TupleInputKVName", pipeline.apply("CreateCoGroupInputName", Create.of(inputKVName)))
                .apply(CoGroupByKey.create())
                .apply("CoGroupValuesByKey", ParDo.of(new PrintFn()))
        ;
    }
    public static void coGroupByKeyAdvanced(Pipeline pipeline,
                                            List<KV<String, Integer>> inputKV,
                                            List<KV<String, String>> inputKVName,
                                            List<KV<String, String>> inputKVColor) {
        // Add tags in order to differentiate after combination into PCollection tuple
        KeyedPCollectionTuple
                // Combine 3 KVs into PCollection tuple
                .of("TupleKV", pipeline.apply("CoGroupInput", Create.of(inputKV))
                        .apply(Sum.integersPerKey())) // sum values of inputKV
                .and("TupleKVName", pipeline.apply("CoGroupInputName", Create.of(inputKVName)))
                .and("TupleKVColor", pipeline.apply("CoGroupInputColor", Create.of(inputKVColor)))
                .apply(CoGroupByKey.create())
                // Create customized output
                .apply("CoGroupValuesByKeyAdvanced", ParDo.of(new DoFn<KV<String, CoGbkResult>, Void>() {
                    private final Logger LOG = LoggerFactory.getLogger(getClass());
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        // Could use (Multi)OutputReceiver out as input here and return out.get().output()
                        KV<String, CoGbkResult> e = Objects.requireNonNull(c.element());
                        String key = e.getKey();
                        CoGbkResult coGbkResult = Objects.requireNonNull(e.getValue());
                        // Could define TupleTag<String> TupleKVTag = new TupleTag<>(); before
                        // So use getOnly(TupleKVTag) instead of getOnly("TupleKV") here
                        String sum = Objects.requireNonNull(coGbkResult.getOnly("TupleKV")).toString();
                        String name = Objects.requireNonNull(coGbkResult.getOnly("TupleKVName")) + "s";
                        String color = Objects.requireNonNull(coGbkResult.getOnly("TupleKVColor"));
                        color = color.substring(0, 1).toUpperCase() + color.substring(1);
                        // Output processed elements
                        // key icon + sum number of element + capitalized first letter color + element name (s)
                        LOG.info(key + " " + sum + " " + color + " " + name);
                    }
                }))
        ;
    }
    public static void combine(Pipeline pipeline, List<Integer> input) {
        class SumInteger implements SerializableFunction<Iterable<Integer>, Integer> {
            @Override
            public Integer apply(Iterable<Integer> input) {
                if (input == null) {
                    return null;
                }
                int sum = 0;
                for (int i : input) {
                    sum += i;
                }
                return sum;
            }
        }
        pipeline
                .apply("CreateCombineInput", Create.of(input))
                .apply(Combine.globally(new SumInteger()))
                .apply("OutputCombineResult", ParDo.of(new PrintFn()))
        ;
    }
    public static void combineFn(Pipeline pipeline, List<String> inputString) {
        pipeline
                .apply("CreateCombineFnInput", Create.of(inputString))
                .apply(Combine.globally(new AverageWordLengthFn()))
                .apply("OutputCombineFnResult", ParDo.of(new PrintFn()))
        ;
    }
    public static void combinePerKey(Pipeline pipeline, List<KV<String, Integer>> inputKV) {
        // Apply CombineFn to each key of a PCollection of key-value pairs
        pipeline
                .apply("CreateCombinePerKeyInput", Create.of(inputKV))
                // Could initiate class extends BinaryCombineFn outside of this statement
                .apply(Combine.perKey(new BinaryCombineFn<>() {
                    @Override
                    public Integer apply(Integer left, Integer right) {
                        return left + right;
                    }
                }))
                .apply("OutputCombinePerKeyResult", ParDo.of(new PrintFn()))
        ;
    }
    public static void composite(Pipeline pipeline, String inputWord) {
        pipeline
                .apply("CreateCompositeInput", Create.of(inputWord))
                .apply("ExtractNonSpaceCharacters", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] words = Objects.requireNonNull(c.element()).split(" ");
                        for (String word : words) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply("FilterByPrefix", Filter.by((SerializableFunction<String, Boolean>) word
                        -> word != null ? word.startsWith("H") : null))
                .apply("OutputCompositeResult", ParDo.of(new PrintFn()))
        ;
    }
    public static void flatten(Pipeline pipeline,
                               List<KV<String, String>> inputKVName,
                               List<KV<String, String>> inputKVColor) {
        PCollectionList
                .of(pipeline.apply("FlattenInputKVName", Create.of(inputKVName)))
                .and(pipeline.apply("FlattenInputKVColor", Create.of(inputKVColor)))
                .apply(Flatten.pCollections())
                .apply("OutputFlattenResult", ParDo.of(new PrintFn()))
        ;
    }
    public static void partition(Pipeline pipeline, List<KV<String, Integer>> inputKV) {
        PCollectionList<KV<String, Integer>> partition = pipeline
                .apply("PartitionInputKV", Create.of(inputKV))
                .apply(Partition.of(
                        2, (PartitionFn<KV<String, Integer>>) (KV, numPartitions) -> {
                            if (KV == null || KV.getValue() == null) {
                                return 0;
                            } else {
                                return KV.getValue() <= 3 ? 0 : 1;
                            }
                        })
                )
        ;
        partition.get(0).apply("OutputPartitionResult0", ParDo.of(new PrintFn("element <= 3")));
        partition.get(1).apply("OutputPartitionResult1", ParDo.of(new PrintFn("element > 3")));
    }
    public static void sideInputs(Pipeline pipeline,
                                  List<KV<String, String>> inputKVName,
                                  List<KV<String, String>> inputKVColor) {
        PCollectionView<Map<String, String>> itemToNameView = pipeline
                .apply("SideInputKVName", Create.of(inputKVName))
                .apply(View.asMap())
        ;
        pipeline
                .apply("SideInputKVColor", Create.of(inputKVColor))
                .apply("SideInputProcessing", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Map<String, String> itemToName = Objects.requireNonNull(c.sideInput(itemToNameView));
                        KV<String, String> e = Objects.requireNonNull(c.element());
                        String item = e.getKey();
                        String color = e.getValue();
                        String name = itemToName.get(item); // static
                        c.output(KV.of(item, name + " (" + color + ")"));
                    }
                }).withSideInputs(itemToNameView))
                .apply("OutputSideInputsResult", ParDo.of(new PrintFn()))
        ;
    }

    public static void main(String[] args) {
        // Build Pipeline
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(App.Options.class);
        var pipeline = Pipeline.create(options);

        // Transform Inputs
        List<Integer> input = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<KV<String, Integer>> inputKV = Arrays.asList(
                KV.of("🍰", 3),
                KV.of("🍰", 2),
                KV.of("🥥", 1),
                KV.of("🍎", 4),
                KV.of("🍎", 5),
                KV.of("🍎", 3)
        );
        String inputWord = "Hello World";
        List<String> inputString = List.of("a", "b", "c", "hello", "world");
        List<KV<String, String>> inputKVName = Arrays.asList(
                KV.of("🍰", "Cake"),
                KV.of("🥥", "Coconut"),
                KV.of("🍎", "Apple")
        );
        List<KV<String, String>> inputKVColor = Arrays.asList(
                KV.of("🍰", "yellow"),
                KV.of("🥥", "white"),
                KV.of("🍎", "red")
        );

        // 1. Filter - Only get even numbers, no order applied
        TourOfBeam.filter(pipeline, input);
        // 2. Count Aggregation - Count number of elements in the entire PCollection
        TourOfBeam.count(pipeline, input);
        // 3. Sum Aggregation - Sum values for each key in the PCollection of key-value pairs
        TourOfBeam.sum(pipeline, inputKV);
        // 4. ParDo Map - Count word length after splitting
        TourOfBeam.parDoOneToMany(pipeline, inputWord);
        // 5. ParDo Map - Count length for each element
        TourOfBeam.mapElements(pipeline, inputString);
        // 6. ParDo FlatMap - Output key for each key-value pair
        TourOfBeam.flatMapElements(pipeline, inputKV);
        // 7. ParDo Map Group by Key - Group by key and combine values
        TourOfBeam.groupByKey(pipeline, inputKV);
        // 8. ParDo Map CoGroup by Key - Group by key across maps
        TourOfBeam.coGroupByKey(pipeline, inputKV, inputKVName);
        // 9. ParDo Map CoGroup by Key - Group by key across maps and apply processing
        TourOfBeam.coGroupByKeyAdvanced(pipeline, inputKV, inputKVName, inputKVColor);
        // 10. Combine - Compute sum of each element
        TourOfBeam.combine(pipeline, input);
        // 11. Combine Complex - Compute length of each element and output average
        TourOfBeam.combineFn(pipeline, inputString);
        // 12. Combine per Key - Group by key and sum values
        TourOfBeam.combinePerKey(pipeline, inputKV);
        // 13. Composite (Multiple Transforms) - Split and filter
        TourOfBeam.composite(pipeline, inputWord);
        // 14. Flatten - Merges multiple PCollections objects, keys will duplicate
        TourOfBeam.flatten(pipeline, inputKVName, inputKVColor);
        // 15. Partition - Divide PCollections elements based on partition function
        TourOfBeam.partition(pipeline, inputKV);
        // 16. Side Inputs - Access additional input each time it processes an element
        TourOfBeam.sideInputs(pipeline, inputKVName, inputKVColor);

        // Run Pipeline
        pipeline.run().waitUntilFinish();
    }

}