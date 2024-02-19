-- Load the pages.csv dataset
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Load the access_logs.csv dataset
access_logs = LOAD 'Data/access_logs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);

-- Join pages and access_logs on WhatPage to get Name and Nationality
joined_data = JOIN access_logs BY WhatPage, pages BY PersonID;

-- Group by page and calculate the count of accesses
access_count = GROUP joined_data BY (pages::PersonID, pages::Name, pages::Nationality);
page_access_count = FOREACH access_count GENERATE group AS page_info, COUNT(joined_data) AS access_count;

-- Order by access_count in descending order
ordered_data = ORDER page_access_count BY access_count DESC;

-- Limit to the top 10 pages
top_10_pages = LIMIT ordered_data 10;

-- Project the required columns (PersonID, Name, Nationality, access_count)
result = FOREACH top_10_pages GENERATE FLATTEN(page_info) AS (PersonID, Name, Nationality), access_count;

-- Store the result
STORE result INTO 'Output/TaskB_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP result;
