package org.example;

//import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AppTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void appRuns() {
        var elements = App.buildPipeline(pipeline, "Test");

        // Note that the order of the elements doesn't matter.
        PAssert.that(elements).containsInAnyOrder("Test", "Hello", "World!");
        pipeline.run().waitUntilFinish();
    }
}