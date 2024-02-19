-- Load the pages.csv dataset
pages = LOAD 'Data/pages.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (PersonID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);

-- Filter Facebook users with the specified Nationality
filtered_pages = FILTER pages BY Nationality == 'Senegal';

-- Project only the required columns (name and hobby)
result = foreach filtered_pages generate Name, Hobby;

-- Store the result
STORE result INTO 'Output/TaskA_Result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');

-- Display the result
DUMP result;