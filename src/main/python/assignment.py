from pyspark.sql import SparkSession
import pyspark.sql.functions as Func
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()


########################################################################################################################
# Function Name: read_csv_file
# Description: This Function reads the data from csv files from the file path and loads the csv data into dataframe and returns it
# Template: read_csv_file(str) --> dataframe
# Example: read_csv_file('C:/Users/sang0001/IdeaProjects/Maveric/data/input/P_FIIFA19') --> dataframe
########################################################################################################################
def read_csv_file(file_path, header_flag=True):
    return spark.read.csv(file_path, header=header_flag)

########################################################################################################################
# Function Name: get_most_left_footed_midfielders_club_under30_age
# Description: This Function get the list of club names which has the most number of left footed midfielders under 30 years of age
# Template: get_most_left_footed_midfielders_club_under30_age(dataframe)--> List
# Example: read_csv_file(input_df) --> ['A', 'B']
########################################################################################################################
def get_most_left_footed_midfielders_club_under30_age(df):
    fltr_df = df.filter("Age<30 and PreferredFoot='Left' and Position in ('CAM','LAM','RAM','LM','RM','CM','LCM','RCM','CDM','LDM','RDM')")
    agg_df = fltr_df.groupBy("Club").agg(Func.count("Name").alias("Number_of_players"))
    max_number_of_players = agg_df.groupBy().max("Number_of_players").collect()[0].asDict()['max(Number_of_players)']
    #max_number_of_players = 6
    return [club.asDict()['Club'] for club in agg_df.filter("Number_of_players="+str(max_number_of_players)).select("Club").collect()]

########################################################################################################################
# Function Name: format_currency
# Description: This Function converts the currency from string to integers
# Template: format_currency(str) --> int
# Example: format_currency('€110.5M') --> 110500000
########################################################################################################################
def format_currency(currency):
    mult = {
        "K": 1000,
        "M": 1000000,
        "B": 1000000000
    }
    if 'K' in currency or 'M' in currency or 'B' in currency:
        return int(float(currency[1:-1])*mult.get(currency[-1:], 1))
    else:
        return int(currency[1:])

########################################################################################################################
# Function Name: get_most_expensive_squad
# Description: This Function tells us which team has the most expensive squad value in the world? and Does that team also have the largest wage bill
# Template: get_most_expensive_squad(datframe)--> dict
# Example: get_most_expensive_squad(input_df) --> {"Expensive Squad": "Chelsea", "Is same team is also have the largest wage bill": False}
########################################################################################################################
def get_most_expensive_squad(df):
    format_currency_func = Func.udf(format_currency, LongType())
    frmt_df = df.withColumn("formated_value", format_currency_func("Value")).withColumn("formated_wage", format_currency_func("Wage"))
    agg_df = frmt_df.groupBy("Club").agg(Func.sum("formated_value").alias("Total_Value"), Func.sum("formated_wage").alias("Total_Wage"))
    # agg_df.groupBy().max("Number_of_players").collect()[0].asDict()['max(Number_of_players)']
    expensive_squad_value = agg_df.groupBy().max("Total_Value").collect()[0].asDict()['max(Total_Value)']
    expensive_squad = agg_df.filter("Total_Value="+str(expensive_squad_value)).select("Club").collect()[0].asDict()['Club']
    largest_wage_bill = agg_df.groupBy().max("Total_Wage").collect()[0].asDict()['max(Total_Wage)']
    largest_wage_squd = agg_df.filter("Total_Wage="+str(largest_wage_bill)).select("Club").collect()[0].asDict()['Club']
    #print(expensive_squad)
    #print(largest_wage_squd)
    return {"Expensive Squad":expensive_squad, "Is same team is also have the largest wage bill": expensive_squad==largest_wage_squd }

########################################################################################################################
# Function Name: get_postion_with_highest_wage_in_avg
# Description: This Function tells us which position pays the highest wage in average?
# Template: get_postion_with_highest_wage_in_avg(dataframe) --> str
# Example: get_postion_with_highest_wage_in_avg(input_df) --> LF
########################################################################################################################
def get_postion_with_highest_wage_in_avg(df):
    format_currency_func = Func.udf(format_currency, LongType())
    frmt_df = df.withColumn("formated_wage", format_currency_func("Wage"))
    agg_df = frmt_df.groupBy("Position").agg(Func.avg("formated_wage").alias("Average_Wage"))
    highest_avg_wage_bill = agg_df.groupBy().max("Average_Wage").collect()[0].asDict()['max(Average_Wage)']
    return agg_df.filter("Average_Wage="+str(highest_avg_wage_bill)).select("Position").collect()[0].asDict()["Position"]

########################################################################################################################
# Function Name: get_goal_skill
# Description: This Function calculates the goal skill of the player
# Template: get_goal_skill(str, str, str, str, str) --> float
# Example: get_goal_skill('96', '98', '91', '89', '93') --> 93.4
########################################################################################################################
def get_goal_skill(GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes):
    if GKDiving is None: GKDiving=0
    if GKHandling is None: GKHandling=0
    if GKKicking is None: GKKicking=0
    if GKPositioning is None: GKPositioning=0
    if GKReflexes is None: GKReflexes=0
    return float(int(GKDiving)+int(GKHandling)+int(GKKicking)+int(GKPositioning)+int(GKReflexes))/5

########################################################################################################################
# Function Name: top_5_goal_keeper
# Description: This Function gets the top 5 goal keepers
# Template: top_5_goal_keeper(dataframe) --> list
# Example: top_5_goal_keeper(input_df) -->  ['A', 'B', 'C', 'D', 'E']
########################################################################################################################
def top_5_goal_keeper(df):
    goal_skill_func = Func.udf(get_goal_skill, FloatType())
    frmt_df = df.withColumn("goal_skill", goal_skill_func("GKDiving","GKHandling","GKKicking","GKPositioning","GKReflexes"))
    return [name.asDict()['Name'] for name in frmt_df.sort("goal_skill", ascending=0).limit(5).select("Name").collect()]

########################################################################################################################
# Function Name: get_striker_skill
# Description: This Function calculates the strike skill of the player
# Template: get_striker_skill(str,str,str,str,str,str) --> float
# Example: get_striker_skill('96', '98', '91', '89', '93', '92') --> 93.1666666667
########################################################################################################################
def get_striker_skill(BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance):
    if BallControl is None: BallControl=0
    if Acceleration is None: Acceleration=0
    if SprintSpeed is None: SprintSpeed=0
    if Agility is None: Agility=0
    if Reactions is None: Reactions=0
    if Balance is None: Balance=0
    return float(int(BallControl)+int(Acceleration)+int(SprintSpeed)+int(Agility)+int(Reactions)+int(Balance))/6

########################################################################################################################
# Function Name: top_5_striker
# Description: This Function gets the top 5 strikers
# Template: top_5_striker(dataframe) --> list
# Example: top_5_striker('input_df') --> ['A', 'B', 'C', 'D', 'E']
########################################################################################################################
def top_5_striker(df):
    striker_skill_func = Func.udf(get_striker_skill, FloatType())
    frmt_df = df.withColumn("striker_skill", striker_skill_func("BallControl","Acceleration","SprintSpeed","Agility","Reactions","Balance"))
    return [name.asDict()['Name'] for name in frmt_df.sort("striker_skill", ascending=0).limit(5).select("Name").collect()]


input_file_path = 'C:/Users/sang0001/IdeaProjects/Maveric/data/input/P_FIFA19'
input_df = read_csv_file(input_file_path)

print("#"*100)
print("Q.2: a) Which club has the most number of left footed midfielders under 30 years of age?")
print(get_most_left_footed_midfielders_club_under30_age(input_df))
print("")
print("Q.2: c) Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?")
print(get_most_expensive_squad(input_df))
print("")
print("Q.2: d) Which position pays the highest wage in average?")
print(get_postion_with_highest_wage_in_avg(input_df))
print("")
print("Q.2: e) What makes a goalkeeper great? Share 4 attributes which are most relevant to becoming a good goalkeeper?")
print("Ans: GKDiving,GKHandling,GKKicking,GKPositioning,GKReflexes These are the attributes which are most relevant to becoming a good goalkeeper")
print("Below are the top 5 Goal Keeps by considering above 5 attributes")
print(top_5_goal_keeper(input_df))
print("")
print("Q.2: f) What makes a good Striker (ST)? Share 5 attributes which are most relevant to becoming a top striker ?")
print("Ans: BallControl,Acceleration,SprintSpeed,Agility,Reactions,Balance These are the attributes which are most relevant to becoming a top striker")
print("Below are the top 5 Strikers by considering above 6 attributes")
print(top_5_striker(input_df))
