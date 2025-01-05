package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class App {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My input text")
        String getInputText();

        void setInputText(String value);
    }

    public static PCollection<String> buildPipeline(Pipeline pipeline, String inputText) {
        return pipeline
                // Create a PCollection from an in-memory array of strings
                .apply("Create elements", Create.of(Arrays.asList("Hello", "World!", inputText)))
                // Transform that maps the elements of a collection into a new collection
                .apply("Print elements",
                        MapElements.into(TypeDescriptors.strings()).via(x -> {
                            System.out.println(x);
                            return x;
                        }));
    }

    public static void main(String[] args) {
        // Creates a Pipeline object which builds up the graph of transformations to be executed
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        var pipeline = Pipeline.create(options);
        App.buildPipeline(pipeline, options.getInputText());
        // Direct Runner runs the pipeline locally
        pipeline.run().waitUntilFinish();
    }
}