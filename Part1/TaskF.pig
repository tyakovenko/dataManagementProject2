-- Load the pages.csv dataset
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Load the friends.csv dataset
friends = LOAD 'Data/friends.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: chararray, Desc: chararray);

-- Load the access_logs.csv dataset
access_logs = LOAD 'Data/access_logs.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (AccessID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);

-- Join friends and pages to get the names of PersonID and MyFriend
joined_data = JOIN friends BY PersonID, pages BY PersonID;

-- Join the result with access_logs based on ByWho and WhatPage
final_data = JOIN joined_data BY (friends::PersonID, friends::MyFriend) LEFT OUTER, access_logs BY (ByWho, WhatPage);

-- Filter out entries where the owner has accessed their friend's page
filtered_data = FILTER final_data BY access_logs::AccessID IS NULL;

-- Project the required columns (PersonID, Name) from the filtered data
result = DISTINCT (FOREACH filtered_data GENERATE friends::PersonID AS PersonID, pages::Name AS Name);

-- Store the result
STORE result INTO 'Output/TaskF_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP result;