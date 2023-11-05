-- This file contains all the code that I used for cleaning my files through Pig.

-- starbucks-menu-nutrition-drinks.csv
data = LOAD 'starbucks_raw/starbucks-menu-nutrition-drinks.csv' USING PigStorage(',') AS (Name:chararray, Calories:chararray, Fat:chararray, Carb:chararray, Fiber:chararray, Protein:chararray, Sodium:chararray);

-- After dumping the data I saw that there were various fields with a '-' so I used the GENERATE to change these to 0's
data = FOREACH data GENERATE Name,
    (Calories == '-' ? '0' : Calories) AS Calories:chararray,
    (Fat == '-' ? '0' : Fat) AS Fat:chararray,
    (Carb == '-' ? '0' : Carb) AS Carb:chararray,
    (Fiber == '-' ? '0' : Fiber) AS Fiber:chararray,
    (Protein == '-' ? '0' : Protein) AS Protein:chararray,
    (Sodium == '-' ? '0' : Sodium) AS Sodium:chararray;

--Remove headers and save
data = FILTER data BY Name != '';
STORE data INTO 'starbucks_clean/clean_drinks' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- starbucks-menu-nutrition-food.csv -> starbucks-menu-nutrition-food-no_encoding.csv discussed in write up.
data = LOAD 'starbucks_raw/starbucks-menu-nutrition-food-no_encoding.csv' USING PigStorage(',') AS (Name:chararray, Calories:chararray, Fat:chararray, Carb:chararray, Fiber:chararray, Protein:chararray, Sodium:chararray);

--Remove headers and save
data = FILTER data BY Name != '';
store data INTO 'starbucks_clean/starbucks_food' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- starbucks_drinkMenu_expanded.csv
data = LOAD 'starbucks_raw/starbucks_drinkMenu_expanded.csv' USING PigStorage(',') AS (BevCat:chararray, Bev:chararray, Bev_Prep:chararray, Calories:chararray, Fat:chararray, D1:chararray, D2:chararray, Sodium:chararray, Carb:chararray, D3:chararray, Fibre:chararray, D4:chararray, Protein:chararray, D5:chararray, D6:chararray, D7:chararray, D8:chararray, D9:chararray);

--Reomve columns to match other file formats. Remove headers and then save
filtered_data = FOREACH data GENERATE BevCat, Bev, Bev_Prep, Calories, Fat, Sodium, Carb, Fibre, Protein;
data = FILTER data BY BevCat != '';
STORE filtered_data INTO 'starbucks_clean/starbucks_menu' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- All files were saved and stored in my starbucks_clean folder ready for querying.
