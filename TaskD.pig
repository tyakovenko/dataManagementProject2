-- Load the pages.csv dataset
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Load the friends.csv dataset
friends = LOAD 'Data/friends.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (FriendRel: int, PersonID: int, MyFriend: int, DateOfFriendship: chararray, Desc: chararray);

-- Group by MyFriend and calculate the count of friends for each person
friend_counts = GROUP friends BY MyFriend;
connectedness_factor = FOREACH friend_counts GENERATE group AS PersonID, COUNT(friends) AS FriendCount;

-- Perform a full outer join
result = JOIN connectedness_factor BY PersonID RIGHT, pages BY PersonID;

-- If there are no friends for a person, set the FriendCount to 0
result_with_zero = FOREACH result GENERATE pages::Name AS Name, (connectedness_factor::FriendCount is null ? 0 : connectedness_factor::FriendCount) AS FriendCount;

-- Store the result
STORE result_with_zero INTO 'Output/TaskD_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP result_with_zero;
