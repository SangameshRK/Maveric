from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.getOrCreate()

########################################################################################################################
# Function Name: read_csv_file
# Description: This Function reads the data from csv files from the file path and loads the csv data into dataframe and returns it
# Template: read_csv_file(str) --> dataframe
# Example: read_csv_file('C:/Users/sang0001/IdeaProjects/Maveric/data/input/P_FIIFA19') --> dataframe
########################################################################################################################
def read_csv_file(file_path, header_flag=True):
    return spark.read.csv(file_path, header=header_flag)


if __name__ == '__main__':
    incremental_data_path = 'C:/Users/sang0001/IdeaProjects/Maveric/data/input/INC_FIFA19'
    persisted_data_path = 'C:/Users/sang0001/IdeaProjects/Maveric/data/input/P_FIFA19_TEST'
    temp_data_path = 'C:/Users/sang0001/IdeaProjects/Maveric/data/input/T_FIFA19'
    primary_key = 'ID'
    ####################################################################################################################
    # I. Check if the incremental directory is present and there are files in it
    #    A. If above conditions is true, Then
    #       i. Read incremental data to a dataframe
    #       ii. Check if the persisted data directory is present and there are files in it
    #           a. If the above conditions are true, Then
    #              1. Filter the ID's from incremental data in the persisted dataframe using left outer join and filter
    #              2. Union the filtered persisted data and the incremental data
    #              3. Write unioned data to temp location
    #           b. Else,
    #              1 Write incremental dataframe to temp location
    #       iii. Clean persisted data directory
    #        iv. Move data from temp location to persisted data directory
    ####################################################################################################################
    if os.path.exists(incremental_data_path) and os.listdir(incremental_data_path):
        incr_df = read_csv_file(incremental_data_path)
        if os.path.exists(persisted_data_path) and os.listdir(persisted_data_path):
            pers_df = read_csv_file(persisted_data_path)
            scd1_df = pers_df.join(incr_df, on=primary_key, how='left').filter(incr_df.ID.isNull()).select(pers_df["*"]).union(incr_df)
            scd1_df.repartition(1).write.mode('overwrite').csv(temp_data_path)
        else:
            incr_df.repartition(1).write.mode('overwrite').csv(temp_data_path)

        cmd_clean_pers_data = 'rm -rf '+persisted_data_path+'/*'
        cmd_copy_temp_data = 'cp -r '+temp_data_path+'/* '+persisted_data_path+'/'
        print(cmd_clean_pers_data)
        print(cmd_copy_temp_data)
        os.system(cmd_clean_pers_data)
        os.system(cmd_copy_temp_data)

