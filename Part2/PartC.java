double threshold = 5.0;
boolean hasConverged = false;

for (int iteration = 1; iteration <= 20 && !hasConverged; iteration++) {
    // Run MapReduce job
    // Calculate movement of each centroid
    // If all movements < threshold, set hasConverged = true
}