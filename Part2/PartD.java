// Combiner
class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
    reduce(Text key, Iterable<Text> values, Context context) {
        // Aggregate points for the same centroid locally
        // Emit partial aggregated results
    }
}
for (int iteration = 1; iteration <= 20 && !hasConverged; iteration++) {
    // Run MapReduce job
    // Include mapper in configuration
    job1.setJarByClass(KMeansSingleIteration.class);
    job1.setMapperClass(KMeansMapper.class);
    job1.setCombinerClass(KMeansCombiner.class)
    job1.setReducerClass(KMeansReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    // Calculate movement of each centroid
    // If all movements < threshold, set hasConverged = true
}