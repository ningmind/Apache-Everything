package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.function.PrintFn;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/*
This is the second part of examples rewrite for TourOfBeam https://tour.beam.apache.org/tour/java
Mainly covers schema-based transforms, windowing, triggers, IO connectors and other transforms
*/
public class TourOfBeamAdvanced {
    private static final Logger LOG = LoggerFactory.getLogger(TourOfBeamAdvanced.class);
    @DefaultSchema(JavaFieldSchema.class)
    public static class Game {
        public String userId;
        public Integer score;
        public String gameId;
        public String date;
        @SchemaCreate
        public Game(String userId, Integer score, String gameId, String date) {
            this.userId = userId;
            this.score = score;
            this.gameId = gameId;
            this.date = date;
        }
        public String getDate() {
            return date;
        }
        @Override
        public String toString() {
            return "Game{userId=" + userId + ", score=" + score + ", gameId=" + gameId + ", date=" + date + "}";
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            Game game = (Game) o;
            return this.userId.equals(game.userId) && this.score.equals(game.score)
                    && this.gameId.equals(game.gameId) && this.date.equals(game.date);
        }
        @Override
        public int hashCode() {
            return Objects.hash(this.userId, this.score, this.gameId, this.date);
        }
    }
    @DefaultSchema(JavaFieldSchema.class)
    public static class User {
        public String userId;
        public String userName;
        public Game game;
        @SchemaCreate
        public User(String userId, String userName, Game game) {
            this.userId = userId;
            this.userName = userName;
            this.game = game;
        }
        @Override
        public String toString() {
            return "User{userId=" + userId + ", userName=" + userName + ", game=" + game + "}";
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            User user = (User) o;
            return this.userId.equals(user.userId)
                    && this.userName.equals(user.userName) && this.game.equals(user.game);
        }
        @Override
        public int hashCode() {
            return Objects.hash(this.userId, this.userName, this.game);
        }
    }
    @DefaultSchema(JavaFieldSchema.class)
    public static class shortInfo {
        public String userId;
        public String userName;
        @SchemaCreate
        public shortInfo(String userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }
        @Override
        public String toString() {
            return "User{userId=" + userId + ", userName=" + userName + "}";
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }
            shortInfo shortinfo = (shortInfo) o;
            return this.userId.equals(shortinfo.userId) && this.userName.equals(shortinfo.userName);
        }
        @Override
        public int hashCode() {
            return Objects.hash(this.userId, this.userName);
        }
    }
    public static class ExtractUserProgressFn extends DoFn<String, User> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = Objects.requireNonNull(c.element()).split(",");
            // Example: user16_AmaranthKoala,AmaranthKoala,18,1447719060000,2015-11-16 16:11:03.921
            c.output(new User(
                    items[0], // userId (e.g. user16_AmaranthKoala)
                    items[1], // userName (e.g. AmaranthKoala)
                    new Game(
                            items[0], // userId (e.g. user16_AmaranthKoala)
                            Integer.valueOf(items[2]), // score (e.g. 18)
                            items[3], // gameId (e.g. 1447719060000)
                            items[4] // date (e.g. 2015-11-16 16:11:03.921)
                    )
            ));
        }
    }
    public static class CustomCoder extends Coder<User> {
        private static final CustomCoder INSTANCE = new CustomCoder();
        public static CustomCoder of() {
            return INSTANCE;
        }
        @Override
        public void encode(User user, OutputStream outStream) throws IOException {
            assert user != null;
            String line = user.userId + "," + user.userName + ";"
                    + user.game.score + "," + user.game.gameId + "," + user.game.date;
            outStream.write(line.getBytes());
        }
        @Override
        public User decode(@Nullable InputStream inStream) throws IOException {
            assert inStream != null;
            String serializedDTOs = new String(StreamUtils.getBytesWithoutClosing(inStream));
            String[] params = serializedDTOs.split(";");
            String[] user = params[0].split(",");
            String[] game = params[1].split(",");
            return new User(user[0], user[1], new Game(user[0], Integer.valueOf(game[0]), game[1], game[2]));
        }
        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }
        @Override
        public void verifyDeterministic() {
        }
    }

    public static void main(String[] args) {
        // Build Pipeline
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(App.Options.class);
        var pipeline = Pipeline.create(options);

        // 17. Create Schema
        // Inputs from http://storage.googleapis.com/apache-beam-samples/game/small/gaming_data.csv
        PCollection<String> rides = pipeline.apply(TextIO.read().from(
                "src/test/java/org/example/download/gaming_data.csv"
        ));
        PCollection<String> fullString = rides
                // PTransform<PCollection<String>, PCollection<Iterable<String>>> sample
                .apply(Sample.fixedSizeGlobally(10)) // Get 10 random sample records
                .apply(Flatten.iterables());
        PCollection<User> fullCoder = fullString
                .apply("fullCoder", ParDo.of(new ExtractUserProgressFn()));
        PCollection<User> fullStatistics = fullString // another PCollection<User> to avoid Coder conflict
                .apply("fullStatistics", ParDo.of(new ExtractUserProgressFn()));
        // 17.1 Coder output
        PCollection<User> inputCoder = fullCoder.setCoder(Objects.requireNonNull(CustomCoder.of()));
        inputCoder.apply("OutputUserCoder", ParDo.of(new PrintFn("User sample")));
        // 17.2 Schema output
        Schema shortInfoSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .build();
        Schema gameSchema = Schema.builder()
                .addStringField("userId")
                .addInt32Field("score")
                .addStringField("gameId")
                .addStringField("date")
                .build();
        Schema userSchema = Schema.builder()
                .addStringField("userId")
                .addStringField("userName")
                .addRowField("game", gameSchema)
                .build();
        PCollection<User> input = fullStatistics.setSchema(
                userSchema,
                TypeDescriptor.of(User.class),
                row -> {
                    User user = Objects.requireNonNull(row);
                    Game game = user.game;
                    Row gameRow = Row.withSchema(gameSchema)
                            .addValues(game.userId, game.score, game.gameId, game.date).build();
                    return Row.withSchema(userSchema)
                            .addValues(row.userId, row.userName, gameRow).build();
                },
                row -> {
                    String userId = row.getValue("userId");
                    String userName = row.getValue("userName");
                    Row game = Objects.requireNonNull(row.getValue("game"));
                    String gameId = game.getValue("gameId");
                    Integer score = game.getValue("score");
                    String date = game.getValue("date");
                    return new User(userId, userName, new Game(userId, score, gameId, date));
                }
        );
        // 18. Select - Get fields or nested fields
        input
                .apply("[userId] and [userName]",
                        Select.<User>fieldNames("userId", "userName").withOutputSchema(shortInfoSchema))
                .apply("OutputShortUserGameInfo", ParDo.of(new PrintFn("Short Info Sample")));
        input
                .apply("user [game]", Select.fieldNames("game.*"))
                .apply("OutputGameSchema", ParDo.of(new PrintFn("Game Sample")));
        input
                .apply("All fields", Select.flattenedSchema())
                .apply("OutputFlattenedUserSchema", ParDo.of(new PrintFn("Flattened User Sample")));
        input
                .apply("[userName] and [game.score]", Select.fieldNames("userName", "game.score"))
                .apply("OutputUserGameScore", ParDo.of(new PrintFn("User Game Score Sample")));
        // 19. Join - Separate above defined schema and then join together
        PCollection<shortInfo> getShortInfo = fullString
                .apply("ParDo shortInfo", ParDo.of(new DoFn<String, shortInfo>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] items = Objects.requireNonNull(c.element()).split(",");
                c.output(new shortInfo(items[0], items[1]));
            }
        }));
        PCollection<Game> getGame = fullString
                .apply("ParDo game", ParDo.of(new DoFn<String, Game>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String[] items = Objects.requireNonNull(c.element()).split(",");
                c.output(new Game(items[0], Integer.valueOf(items[2]), items[3], items[4]));
            }
        }));
        PCollection<shortInfo> shortInfoInput = getShortInfo.setSchema(
                shortInfoSchema,
                TypeDescriptor.of(shortInfo.class),
                row -> {
                    shortInfo shortinfo = Objects.requireNonNull(row);
                    return Row.withSchema(shortInfoSchema)
                            .addValues(shortinfo.userId, shortinfo.userName).build();
                }, row -> new shortInfo(row.getString(0), row.getString(1))
        );
        PCollection<Game> gameInput = getGame.setSchema(
                gameSchema,
                TypeDescriptor.of(Game.class),
                row -> {
                    Game game = Objects.requireNonNull(row);
                    return Row.withSchema(gameSchema)
                            .addValues(game.userId, game.score, game.gameId, game.date).build();
                }, row -> new Game(row.getString(0),
                        row.getInt32(1), row.getString(2), row.getString(3))
        );
        shortInfoInput
                .apply(Join.<shortInfo, Game>innerJoin(gameInput).using("userId"))
                .apply("OutputUserJoinGame", ParDo.of(new PrintFn("User Game Join Sample")));
        // 20. Group - Group records from one or more fields, could include aggregation
        PCollection<Object> inputObject = inputCoder
                .apply("MapElements", MapElements.into(TypeDescriptor.of(Object.class)).via(it -> it))
                .setSchema(
                        userSchema,
                        TypeDescriptor.of(Object.class),
                        row -> {
                            User user = (User) Objects.requireNonNull(row);
                            Game game = user.game;
                            Row gameRow = Row.withSchema(gameSchema)
                                    .addValues(user.userId, game.score, game.gameId, game.date).build();
                            return Row.withSchema(userSchema)
                                    .addValues(user.userId, user.userName, gameRow).build();
                        },
                        row -> {
                            Row game = Objects.requireNonNull(row.getValue("game"));
                            return new User(row.getString(0), row.getString(1),
                                    new Game(game.getString(0), game.getInt32(1),
                                            game.getString(2), game.getString(3)));
                        }
                );
        inputObject
                .apply(Group.byFieldNames("userId")
                        .aggregateField("game.score", Sum.ofIntegers(), "total"))
                .apply("MapString", MapElements.into(TypeDescriptor.of(String.class)).via(row -> {
                    Row r = Objects.requireNonNull(row);
                    return Objects.requireNonNull(r.getRow(0)).getValue(0)
                            + " : " + Objects.requireNonNull(r.getRow(1)).getValue(0);
                }))
                .apply("OutputGroupFields", ParDo.of(new PrintFn("User Game Group Sample")))
        ;
        // 21. Filter - Filter User with game score > 11
        /*
        Here might encounter error [Creator parameter arg0 Doesn't correspond to a schema field]
        This is due to java is not able to infer parameter names of the @SchemaCreate method
        Solution: add -parameters (IntelliJ path below) and rebuild the project
        Settings > Build, Execution, Deployment > Compiler > Java Compiler > Additional command line parameters
        */
        inputObject
                .apply(Filter.create().whereFieldName("game.score", score ->
                        score != null ? (int) score > 11 : null))
                .apply("MapUser", MapElements.into(TypeDescriptor.of(User.class)).via(user -> (User) user))
                .apply("OutputFilterFields", ParDo.of(new PrintFn("User Game Filter Sample")));
        // 22. CoGroup - Join across multiple schema PCollections
        PCollectionTuple.of("shortInfo", shortInfoInput).and("game", gameInput)
                .apply(CoGroup.join("shortInfo", CoGroup.By.fieldNames("userId").withOptionalParticipation())
                        .join("game", CoGroup.By.fieldNames("userId")).crossProductJoin())
                .apply("OutputCoGroupLeftJoin", ParDo.of(new PrintFn("User Game CoGroup Left Join")));
        PCollectionTuple.of("shortInfoIterable", shortInfoInput).and("gameIterable", gameInput)
                .apply(CoGroup.join(CoGroup.By.fieldNames("userId")))
                .apply("OutputCoGroupIterable", ParDo.of(new PrintFn("User Game CoGroup Iterable")));
        // 23. Convert - Convert between different Java types with same schema, example User to Row
        PCollection<Row> inputRow = input.apply(Convert.to(Row.class));
        inputRow.apply("OutputConvertFields", ParDo.of(new PrintFn("User Game Convert Sample")));
        // 24. Rename - Rename specific fields and left values unchanged
        inputObject.apply(RenameFields.create()
                .rename("userId", "id").rename("userName", "name"));

        // 25. Windowing Timestamp - assign timestamp to each element in PCollection
        pipeline
                .apply("CreateGameInput", Create.of(
                        new Game("user1", 91, "game1", "2024-01-01T00:00:01+00:00"),
                        new Game("user2", 92, "game2", "2024-01-02T00:00:02+00:00"),
                        new Game("user3", 93, "game3", "2024-01-03T00:00:03+00:00")
                ))
                .apply("ParseDatetime", ParDo.of(new DoFn<Game, Game>() {
                    @ProcessElement
                    public void processElement(@Element Game game, OutputReceiver<Game> out) {
                        out.outputWithTimestamp(game,
                                DateTime.parse(game.getDate()).toInstant());
                    }
                }))
                .apply("OutputGameTimestamp", ParDo.of(new PrintFn("Game Timestamp Sample")))
        ;
        // 26. Windowing - Fixed or sliding windows to divide PCollections
        pipeline
                .apply("CreateStringInput", Create.of(
                        "To", "be", "or", "not", "to", "be","that", "is", "the", "question"
                ))
//                .apply("Global Window", Window.into(new GlobalWindows()))
//                .apply("Fixed Window", Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply("Sliding Window", Window.into(SlidingWindows.of(Duration.standardSeconds(30))
                        .every(Duration.standardSeconds(5)))) // Sliding Window
                .apply("OutputWindows", ParDo.of(new PrintFn("String Window Sample")))
        ;
        // 27. Event Time Trigger - Determine when to emit the aggregated results of each window
        Window<String> window = Window.into(Sessions.withGapDuration(Duration.standardSeconds(600)));
        Trigger eventTimeTrigger = AfterWatermark.pastEndOfWindow();
        pipeline
                .apply("CreateStringNumInput1", Create.of("first", "second", "third"))
                .apply("Session Window", window.triggering(eventTimeTrigger)
                        .withAllowedLateness(Duration.ZERO).discardingFiredPanes())
                .apply("OutputEventTimeTrigger", ParDo.of(new PrintFn("Event Time Trigger Sample")))
        ;
        // 28. Composite Trigger - Specify multiple triggers to be used in combination
        Trigger dataDrivenTrigger = AfterPane.elementCountAtLeast(2); // Data Driven Trigger
        Trigger processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(1)); // Processing Trigger
        pipeline
                .apply("CreateStringNumInput2", Create.of("first", "second", "third"))
                .apply(window.triggering(AfterAll.of(Arrays.asList(processingTimeTrigger,dataDrivenTrigger)))
                        .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes())
                .apply("OutputCompositeTrigger", ParDo.of(new PrintFn("Composite Trigger Sample")))
        ;

        // 29. Text IO - Read from and write to local file
        String filePath = "src/test/java/org/example/download/test_file.txt";
        pipeline
                .apply("Test IO Read", TextIO.read().from(filePath))
                .apply("Output Test IO Read", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] line = Objects.requireNonNull(c.element()).split(" ");
                        for (String word : line) {
                            System.out.println(word); // Output in console
                        }
                    }
                }))
        ;
//        pipeline
//                .apply("CreateTestStringInput", Create.of("Hello, World!"))
//                .apply("Test IO Write", TextIO.write().to(filePath)); // Create file

        // Run Pipeline
        pipeline.run().waitUntilFinish();
    }

}
