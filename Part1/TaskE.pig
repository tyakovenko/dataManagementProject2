-- Load the access_logs.csv dataset
access_logs = LOAD 'Data/access_logs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);

-- Group by ByWho and calculate the total number of accesses and distinct pages accessed
access_counts = GROUP access_logs BY ByWho;
owner_access_stats = FOREACH access_counts {
    distinct_pages = DISTINCT access_logs.WhatPage;
    GENERATE group AS PersonID, COUNT(access_logs) AS TotalAccesses, COUNT(distinct_pages) AS DistinctPagesAccessed;
}

-- Store the result
STORE owner_access_stats INTO 'Output/TaskE_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP owner_access_stats;