/*
Assumption: Every row is a different college. With the given dataset there is no way 
of knowing whether the undertaken column for two rows belong to the same college
*/

-- register the piggybank.jar for the CSVExcelStorage function
REGISTER '/home/arvind/pig/pig-0.17.0/lib/piggybank.jar';

-- load the students data set using CsvExcelStorage
stud_data = LOAD '../input/studentDS' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX',
'SKIP_INPUT_HEADER') AS (name:chararray, undertaken:chararray, dob:chararray, stream:chararray, grade:float, state:chararray, city:chararray);

-- filter for those tuples whose undertaken field is 'goverenment'
studs_filtered = FILTER stud_data BY undertaken == 'goverenment';

-- group by all fields because we want to find the tuple count
studs_grouped = GROUP studs_filtered ALL;

-- find the tuple count
college_count =  FOREACH studs_grouped GENERATE COUNT($1) AS college_count;

-- store the output on local file system
STORE college_count INTO '../output/task_3' USING PigStorage(',');
