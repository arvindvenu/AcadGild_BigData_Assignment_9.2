-- register the piggybank.jar for the CSVExcelStorage function
REGISTER '/home/arvind/pig/pig-0.17.0/lib/piggybank.jar';

-- load the students data set using CsvExcelStorage
stud_data = LOAD '../input/studentDS' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX',
'SKIP_INPUT_HEADER') AS (name:chararray, undertaken:chararray, dob:chararray, stream:chararray, grade:float, state:chararray, city:chararray);

-- filter for those students who got grade less than 50%
studs_filtered = FILTER stud_data BY grade < 5;

-- group by all fields because we want to find the tuple count
studs_grouped = GROUP studs_filtered ALL;

-- find the tuple count
studs_count =  FOREACH studs_grouped GENERATE COUNT($1) AS studs_count;

-- store the output on local file system
STORE studs_count INTO '../output/task_1' USING PigStorage(',');
