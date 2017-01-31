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
  #Select the current shape - Indexed by PR_MDB_C
  #PR_MDB_C
  shapesubset <- subset(shapes, shapes$PR_MDB_C == shapeids[a,])
  
  temp = !is.na(over(coordinates, as(shapesubset, "SpatialPolygons")))
  
  if(sum(temp) > 0){
    
    temp <- as.data.frame(temp)
    
    temp <- temp + 0
    
    temp$ConsumerID <- coordinates@data$ConsumerID
    
    temp$ConsumerGeoAddressID <- coordinates@data$ConsumerGeoAddressID
    
    temp <- temp[temp$temp > 0,]
    
    #Drop temp column
    temp$temp <- NULL
    
    temp$PR_MDB_C <- shapeids[a,]
    
    data.frame(ConsumerID = temp$ConsumerID, ConsumerGeoAddressID = temp$ConsumerGeoAddressID, PR_MDB_C = temp$PR_MDB_C)
  }
}

#Read in shape files
shapes <- readOGR("E:/Ruan/Census 2011 Spatial Geography","PR_SA_2011")

#SQL connection
myconn <-odbcConnect("ODBC_INSTANCE_NAME",uid="USER",pwd="PASSWORD")

#Function to read in a part of the input data
ea_assignment <- function(a,shapes){
  #Build query string
  if(a < 2){
    query <- paste("select ConsumerID, ConsumerGeoAddressID, X, Y from ConsumerGeoAddress..MostRecentAddressFinal where ConsumerGeoAddressID < ",a*1000000,sep="")
  }
  else{
    query <- paste("select ConsumerID, ConsumerGeoAddressID, X, Y from ConsumerGeoAddress..MostRecentAddressFinal where ConsumerGeoAddressID < ",a*1000000," and ConsumerGeoAddressID >= ", (a-1)*1000000,sep="")
  }
  
  return (sqlQuery(myconn,query))
}

#Register multi-core backend
registerDoParallel(detectCores())    

#Get the total number of rows in the database
parts <- sqlQuery(myconn, "select count(*) from ConsumerGeoAddress..MostRecentAddressFinal")

#Determine how many runs, doing one million rows at a time, we will need to do
numruns <- ceiling(parts/1000000)

for(a in 1:numruns[[1]]){
  start.time <- Sys.time()
  print(a)
  #Get the n-th million of the data
  coordinates <- ea_assignment(a,shapes)
  
  #Convert data frame to coordinate data type
  coordinates(coordinates) <- c("X","Y")
  
  #Both files are on the same projection reference 
  proj4string(coordinates) <- proj4string(shapes)
  
  #Assign a coordinate to a specific shape 
  #This will be used to associate enumerated areas with actual coordinates
  #The end-result is an enumerated area ID assigned to each coordinate
  shapedataframe <- as.data.frame(shapes)
  
  #Get list of shapeIDs
  #PR_MDB_C refers to the province ID
  shapeids <- as.data.frame(shapedataframe[,"PR_MDB_C"])
  
  #Rename column
  names(shapeids)[1] <- "shapeid"
  
  #create output data frame
  output <- as.data.frame(matrix(0, ncol = 2, nrow = 0))
  
  #Rename column to ConsumerID
  colnames(output)[1] <- c("ConsumerID")
  
  #Rename column to PR_MDB_C
  colnames(output)[2] <- c("PR_MDB_C")
  
  #Loop through all shapeids and check whether a coordinate is in that shape
  output <- foreach(i=1:nrow(shapeids),.packages=c("plyr"),.combine='rbind') %dopar% { 
    ea_assignment_loop(i,shapes,shapeids,coordinates,output)
  }
  
  
  #cat(output,file="C:/Users/rvanzyl/Documents/XDS/Cell C Geo Project/Data/ProvinceTest.csv",append=TRUE,sep = ",")
  
  #Garbage collection statement
  gc()
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
  
  time.taken <- as.data.frame(time.taken)
  
  
  if(a < 2){
    #Output the time taken to a text file
    write.table(time.taken,file="E:/Ruan/Cell C store location/TimeTaken.csv",append=TRUE,sep=",",row.names=F,quote=F)
    #Output ConsumerID and PR_MDB_C pairs to a text file
    write.table(output,file="E:/Ruan/Cell C store location/ProvinceAssignment.csv",append=TRUE,sep=",",row.names=F,quote=F)
  }
  else{
    write.table(time.taken,file="E:/Ruan/Cell C store location/TimeTaken.csv",append=TRUE,sep=",",row.names=F,quote=F,col.names=F)
    write.table(output,file="E:/Ruan/Cell C store location/ProvinceAssignment.csv",append=TRUE,sep=",",row.names=F,quote=F,col.names=F)
  }
}

#Copy output file to server
#This path should be the same as the file which is appended at each iteration of the loop above
#Replace Server_Path with a valid path
#file.copy("Server_Path/EA_AssignmentTest.txt","//sas-server/S$/Ruan/EA_Assignment",overwrite = TRUE)

#Create new table
#sqlQuery(myconn,"create table Ruan..EAAssignmentTest (
#          ConsumerID varchar(255),
#          ConsumerGeoAddressID varchar(255),
#          EA_CODE varchar(255),
#          LastUpdatedDate date)")

#Bulk import file
#sqlQuery(myconn,"BULK INSERT Ruan..EAAssignmentTest FROM '\\\\sas-server\\S$\\Ruan\\EA_Assignment\\EA_AssignmentTest.txt' WITH (FIELDTERMINATOR = ',',ROWTERMINATOR = '\\n')")

#Close database connection
close(myconn)

#Delete data no longer needed
rm(list = c("output", "shapedataframe", "shapeids","coordinates","i","myconn","shapes","packages"))
