# Assignment2-FintechAnalysis
Continued from Assignment 2- FintechAnalysis


1. part1.py
  Merging all the datasets of the 12 teams into one single file, storing the data in '24banks.csv'  and performing data cleaning and pre-processing to remove any   special characters using regex and then lemmatizing the file and storing the result in 'LemmatizedDataS3.csv'.

2. ClustersList.csv 
  This file contains different clusters related to FinTech along with their associated words.

3. DaskPipelining.py
  This file compares the clusters stored in 'ClustersList.csv' with the 'LemmatizedDataS3.csv' file and returns if the  
  particular job position is a Fintech or non-FinTech job category. If it is a Fintech job, it labels the job with the cluster 
  name which it belongs to.
  It is finally pipelined using dask to carry out the tasks in a automated way.
  
  Claat Report: https://bit.ly/2IWCpyd
