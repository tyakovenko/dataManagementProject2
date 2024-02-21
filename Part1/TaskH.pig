-- Load friends data
friends = LOAD 'Data/friends.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: chararray, Desc: chararray);

-- Group friends data by PersonID to get the count of friend relationships per person
grouped_friends = GROUP friends BY PersonID;
friend_counts = FOREACH grouped_friends GENERATE group AS PersonID, COUNT(friends) AS FriendCount;

-- Calculate the average number of friend relationships per person
total_friends = GROUP friend_counts ALL;
avg_friends = FOREACH total_friends GENERATE AVG(friend_counts.FriendCount) AS AverageFriendCount;

-- Filter people with more friend relationships than the average
popular_people = FILTER friend_counts BY FriendCount > avg_friends.AverageFriendCount;

-- Join with pages data to get the names of popular people
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
popular_people_names = JOIN popular_people BY PersonID, pages BY PersonID;

-- Select required fields
result = FOREACH popular_people_names GENERATE popular_people::PersonID, pages::Name, popular_people::FriendCount;

-- Store the result
STORE result INTO 'Output/TaskH_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP result;
