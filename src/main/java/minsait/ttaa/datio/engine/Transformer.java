package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.naming.Field;
import minsait.ttaa.datio.common.naming.PlayerInput;
import minsait.ttaa.datio.common.naming.PlayerOutput;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;


import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = playerCatWindowFunction(df);
        df = potentialVsOverall(df);
        df = filterPlayerCatPotentialOverall(df);

        df = columnSelection(df);


        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
        writeParquet(df);


    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {

        return df.select(
                PlayerInput.shortName.column(),
                PlayerInput.longName.column(),
                PlayerInput.age.column(),
                PlayerInput.heightCm.column(),
                PlayerInput.weightKg.column(),
                PlayerInput.nationality.column(),
                PlayerInput.clubName.column(),
                PlayerInput.overall.column(),
                PlayerInput.potential.column(),
                PlayerInput.teamPosition.column(),
                PlayerOutput.playerCat.column(),
                PlayerInput.potentialVsOverall.column(),
                PlayerOutput.catHeightByPosition.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }
    
    private Dataset<Row> playerCatWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(20), "B")
                .when(rank.$less(50), "C")
                .otherwise("D");

        return df.withColumn(PlayerOutput.playerCat.getName(), rule);
    }

    private Dataset<Row> potentialVsOverall(Dataset<Row> df) {
        Column dif = (df.col("overall")
                .divide(df.col("potential")));

        return df.withColumn(potentialVsOverall.getName(),dif);
    }

    private Dataset<Row> filterPlayerCatPotentialOverall(Dataset<Row> df) {
        Column filterA = df.col("player_cat").equalTo("A");
        Column filterB = df.col("player_cat").equalTo("B");

        Column filterC = df.col("player_cat").equalTo("C")
                .and(df.col("potential_overall").$greater(1.15));

        Column filterD = df.col("player_cat").equalTo("D")
                .and(df.col("potential_overall").$greater(1.25));

        Column condition = filterA.or(filterB).or(filterC).or(filterD);

        return df.filter(condition);
    }

    private static void writeParquet( Dataset<Row> df) {
        df.select(shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column());

        df.coalesce(1).write().parquet(OUTPUT_PATH);
    }




}
