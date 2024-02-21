-- Load the pages.csv dataset
pages = LOAD 'Data/pages.csv' USING  org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Group by Nationality and calculate the count of citizens
country_counts = GROUP pages BY Nationality;
country_facebook_count = FOREACH country_counts GENERATE group AS Nationality, COUNT(pages) AS CitizenCount;

-- Store the result
STORE country_facebook_count INTO 'Output/TaskC_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP country_facebook_count;
