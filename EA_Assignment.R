#Install necessary packages if not already installed
packages <- c("shapefiles","plyr","ggplot2","rgdal","foreach","RODBC","doParallel")
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
  install.packages(setdiff(packages, rownames(installed.packages())))
}

#Assign libraries
library(shapefiles)
library(plyr)
library(ggplot2)
library(rgdal)
library(foreach)
library(RODBC)
library(SOAR)
library(doParallel)

ea_assignment_loop <- function(a,shapes,shapeids,coordinates,output){
  #Select the current shape - Indexed by Shape_Code
  shapesubset <- subset(shapes, shapes$Shape_Code == shapeids[a,])
  
  temp = !is.na(over(coordinates, as(shapesubset, "SpatialPolygons")))
  
  if(sum(temp) > 0){
    
    temp <- as.data.frame(temp)
    
    temp <- temp + 0
    
    temp$ID <- coordinates@data$ID
    
    temp <- temp[temp$temp > 0,]
    
    #Drop temp column
    temp$temp <- NULL
    
    temp$EA_CODE <- shapeids[a,]
    
    data.frame(ID = temp$ID, Shape_Code = temp$Shape_Code)
  }
}

#Read in shape files
shapes <- readOGR("<shape files location>","<layer name>")

#SQL connection
myconn <-odbcConnect("<RODBC connection name>",uid="<uid>",pwd="<password>")

#Output EA characteristics to SQL 
#sqlSave(myconn, shapedataframe, tablename = "EASpecification", append = TRUE)

#Function to read in a part of the input data
ea_assignment <- function(a,shapes){
  #Build query string
  #Only select non-missing coordinates
  #ID is a counter which runs from 1 to the number of rows in the table containing coordinates
  if(a < 2){
    query <- paste("select X, Y from database..table where ID < ",a*1000000," and X != '0' and Y != '0'" ,sep="")
  }
  else{
    query <- paste("select X, Y from database..table where ID < ",a*1000000, " and ID >= ", (a-1)*1000000," and X != '0' and Y != '0'",sep="")
  }
  return (sqlQuery(myconn,query))
}

#Register multi-core backend
registerDoParallel(detectCores())    

#Drop destination table if it exists
#sqlQuery(myconn, "IF OBJECT_ID('database..table', 'U') IS NOT NULL
         #DROP TABLE database..table;")

#Get the total number of rows in the database
parts <- sqlQuery(myconn, "select count(*) from database..table")

#Determine how many runs, doing one million rows at a time, we will need to do
numruns <- ceiling(parts/1000000)

#for(a in 1:numruns[1,1]){
for(a in 1:numruns){
  start.time <- Sys.time()
  print(a)
  #Get the n-th million of the data
  coordinates <- ea_assignment(a,shapes)
  
  #Convert data frame to coordinate data type
  coordinates(coordinates) <- c("X","Y")
  
  #Both files are on the same projection reference 
  proj4string(coordinates) <- proj4string(shapes)
  
  #Assign a coordinate to a specific shape 
  shapedataframe <- as.data.frame(shapes)
  
  #Get list of shapeIDs
  shapeids <- as.data.frame(shapedataframe[,"Shape_Code"])
  
  #Rename column
  names(shapeids)[1] <- "shapeid"
  
  #create output data frame
  output <- as.data.frame(matrix(0, ncol = 3, nrow = 0))
  
  #Rename column to ConsumerID
  colnames(output)[1] <- c("ID")
  
  #Rename column to Shape_Code
  colnames(output)[2] <- c("Shape_Code")
  
  #Rename column to LastUpdatedDate
  colnames(output)[3] <- c("LastUpdatedDate")
  
  #Loop through all shapeids and check whether a coordinate is in that shape
  output <- foreach(i=1:nrow(shapeids),.packages=c("plyr"),.combine='rbind') %dopar% { 
    ea_assignment_loop(i,shapes,shapeids,coordinates,output)
  }
  
  #Output ID and Shape_Code pairs to a text file
  cat(output, file="Path/output.txt", append=TRUE, sep = ",")
  
  #Garbage collection statement
  gc()
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
  
  time.taken <- as.data.frame(time.taken)
  
  #Output the time taken to a text file
  cat(time.taken, file="Path/time_take.txt", append=TRUE, sep = ",")
}

#Close database connection
close(myconn)

#Delete data no longer needed
rm(list = c("output", "shapedataframe", "shapeids","coordinates","i","myconn","shapes","packages"))
