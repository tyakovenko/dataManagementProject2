// Part E1: In Reducer's reduce method

// Modyfiy reducer to check for convergence and emit new centroid if not
class KMeansReducer extends Reducer<Text, Text, Text, Text> {
    reduce(Text key, Iterable<Text> values, Context context) {
        // Calculate new centroid (average x, average y)
        // Emit new centroid
    // Calculate new centroid
    // Check if converged (based on a global variable or context configuration)
    // Emit new centroid and convergence status
}
//Part E2
}
class KMeansReducer extends Reducer<Text, Text, Text, Text> {
    
    reduce(Text key, Iterable<Text> values, Context context) {
    // Aggregate points
    // Emit centroid and list of points belonging to it
}
}