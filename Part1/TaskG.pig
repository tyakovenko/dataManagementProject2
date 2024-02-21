-- Load pages data
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Load access_logs data
access_logs = LOAD 'Data/access_logs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);

-- Extract PersonID, Format AccessTime as date, Get current date and subtract 14 days from it
access_data = FOREACH access_logs GENERATE ByWho AS PersonID, ToDate(AccessTime, 'yyyy-MM-dd HH:mm:ss') AS AccessDate, SubtractDuration(CurrentTime(), 'P14D') as cutoff_date;

-- Filter data for people who accessed site in the last 14 days
people_accessed_last_14_days = FILTER access_data BY AccessDate > cutoff_date;

-- Perform LEFT OUTER JOIN with pages data to include people who accessed their site
joined_data = JOIN pages BY PersonID LEFT OUTER, people_accessed_last_14_days BY PersonID;

-- Filter records where there is no corresponding entry in access_data (i.e., Person never accessed)
disconnected_people = FILTER joined_data BY people_accessed_last_14_days::PersonID IS NULL;

-- Select required fields
final_result = FOREACH disconnected_people GENERATE pages::PersonID AS PersonID, pages::Name AS Name;

-- Store the result
STORE final_result INTO 'Output/TaskG_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP final_result;
