-- register the piggybank.jar for the CSVExcelStorage function
REGISTER '/home/arvind/pig/pig-0.17.0/lib/piggybank.jar';

-- load the students data set using CsvExcelStorage
stud_data = LOAD '../input/studentDS' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX',
'SKIP_INPUT_HEADER') AS (name:chararray, undertaken:chararray, dob:chararray, stream:chararray, grade:float, state:chararray, city:chararray);

-- filter for those students whose state is Alaska
studs_filtered = FILTER stud_data BY state == 'alaska';

-- project only the names of the students as is required in the assignment
studs_names = FOREACH studs_filtered GENERATE name;

-- store the output on local file system
STORE studs_names INTO '../output/task_2' USING PigStorage(',');
