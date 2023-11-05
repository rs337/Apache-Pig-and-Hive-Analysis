-- Simple Queries

-- 1. What food item has the highest protein?
-- Using the cleaned files from the first section of this assignment.
food = LOAD 'starbucks_clean/starbucks_food/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (Name:chararray, Calories:float, Fat:float, Carb:float, Fiber:float, Protein:float);
highest_protein = ORDER food BY Protein DESC;
high_protein_food = LIMIT highest_protein 1;
dump high_protein_food

-- 2. What drinks have more than 250 calories?
drinks = LOAD 'starbucks_clean/clean_drinks/part-m-00000' USING org.apache.pig.piggybank.storage.CSVLoader() AS (Name:chararray, Calories:float, Fat:float, Carb:float, Fiber:float, Protein:float, Sodium:float);
filtered_drinks = FILTER drinks BY (Calories > 250);
sorted_drinks = ORDER filtered_drinks BY Calories DESC;
dump sorted_drinks 