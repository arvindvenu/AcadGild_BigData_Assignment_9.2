-- register the piggybank.jar for the CSVExcelStorage function
REGISTER '/home/arvind/pig/pig-0.17.0/lib/piggybank.jar';

-- load the students data set using CsvExcelStorage
stud_data = LOAD '../input/studentDS' USING
org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX',
'SKIP_INPUT_HEADER') AS (name:chararray, undertaken:chararray, dob:chararray, stream_name:chararray, grade:float, state:chararray, city:chararray);

-- project only the names, state and stream of the students
studs_projected = FOREACH stud_data GENERATE name, state, stream_name;

-- filter for those students whose state is 'oregon' and stream is 'BE'
studs_filtered = FILTER studs_projected BY state == 'oregon' AND stream_name == 'BE';

-- project only names of the filtered tuples
studs_names = FOREACH studs_filtered GENERATE name;

-- store the output on local file system
STORE studs_names INTO '../output/task_4' USING PigStorage(',');
