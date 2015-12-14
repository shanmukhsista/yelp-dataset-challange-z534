#import all the libraries
args = commandArgs(trailingOnly=TRUE)
nsamples = args[1]
persample= args[2] 
library(ggplot2)
library(infotheo)
library(corrplot)
library(caret)
outpath ="/Users/shanmukh/Documents/Projects/PA-1-Machine Learning-Decision Trees/bagging/"
#Script to import and process Glass Data. 
spect.data = read.csv("/Users/shanmukh/Documents/ml-project/spect/input_cleaned/input-train-pa.csv")
#Build a correlation matrix to information about the features. 
#Sample train and test data sets
unlink(paste(outpath, "*"), recursive = TRUE, force = TRUE)
for(i in 1:(as.numeric(nsamples))) 
{
  set.seed(i)
  newSample = spect.data[sample(nrow(spect.data), persample, replace=TRUE ),];
  write.table(newSample,file=paste(outpath, "input-pa-bagging-", i, ".csv"), 
              row.names=FALSE,
              col.names=FALSE,
              sep=",")
  #write this new file csv
}

