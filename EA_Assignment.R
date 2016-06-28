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
  #Select the current shape - Indexed by EA_CODE
  #PR_MDB_C
  shapesubset <- subset(shapes, shapes$EA_CODE == shapeids[a,])
  
  temp = !is.na(over(coordinates, as(shapesubset, "SpatialPolygons")))
  
  if(sum(temp) > 0){
    
    temp <- as.data.frame(temp)
    
    temp <- temp + 0
    
    temp$ConsumerID <- coordinates@data$ConsumerID
    
    temp$ConsumerGeoAddressID <- coordinates@data$ConsumerGeoAddressID
    
    temp <- temp[temp$temp > 0,]
    
    #Drop temp column
    temp$temp <- NULL
    
    temp$EA_CODE <- shapeids[a,]
    
    data.frame(ConsumerID = temp$ConsumerID, ConsumerGeoAddressID = temp$ConsumerGeoAddressID, EA_CODE = temp$EA_CODE)
  }
}

#Read in shape files
shapes <- readOGR("D:/GeoSpacial/Census 2011 Spatial Geography","EA_SA_2011")

#SQL connection
myconn <-odbcConnect("SAS-Server",uid="sa",pwd="P@ssw0rd")

#Output EA characteristics to SQL 
#sqlSave(myconn, shapedataframe, tablename = "EASpecification", append = TRUE)

#Function to read in a part of the input data
ea_assignment <- function(a,shapes){
  #Build query string
  #Only select addresses that have non-missing coordinates
  if(a < 2){
    query <- paste("select ConsumerID, ConsumerGeoAddressID, X, Y from ConsumerGeoAddress..ConsumerGeoAddress where ConsumerGeoAddressID < ",a*1000000," and X != '0' and Y != '0'" ,sep="")
  }
  else{
    query <- paste("select ConsumerID, ConsumerGeoAddressID, X, Y from ConsumerGeoAddress..ConsumerGeoAddress where ConsumerGeoAddressID < ",a*1000000, " and ConsumerGeoAddressID >= ", (a-1)*1000000," and X != '0' and Y != '0'",sep="")
  }
  
  return (sqlQuery(myconn,query))
}

#Register multi-core backend
registerDoParallel(detectCores())    

#Drop EAAssignment table if it exists
#sqlQuery(myconn, "IF OBJECT_ID('ConsumerGeoAddress..EAAssignment', 'U') IS NOT NULL
         #DROP TABLE ConsumerGeoAddress..EAAssignment;")

#Get the total number of rows in the database
parts <- sqlQuery(myconn, "select count(*) from ConsumerGeoAddress..ConsumerGeoAddress")

#Determine how many runs, doing one million rows at a time, we will need to do
numruns <- ceiling(parts/1000000)

#for(a in 1:numruns[1,1]){
for(a in 1:50){
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
  #EA_CODE refers to the enumerated area ID
  shapeids <- as.data.frame(shapedataframe[,"EA_CODE"])
  
  #Rename column
  names(shapeids)[1] <- "shapeid"
  
  #create output data frame
  output <- as.data.frame(matrix(0, ncol = 3, nrow = 0))
  
  #Rename column to ConsumerID
  colnames(output)[1] <- c("ConsumerID")
  
  #Rename column to ConsumerID
  colnames(output)[2] <- c("ConsumerGeoAddressID")
  
  #Rename column to EA_CODE
  colnames(output)[3] <- c("EA_CODE")
  
  #Rename column to LastUpdatedDate
  colnames(output)[4] <- c("LastUpdatedDate")
  
  #Loop through all shapeids and check whether a coordinate is in that shape
  output <- foreach(i=1:nrow(shapeids),.packages=c("plyr"),.combine='rbind') %dopar% { 
    ea_assignment_loop(i,shapes,shapeids,coordinates,output)
  }
  
  #Output ConsumerID and EA_CODE pairs to a text file
  #Replace Server_Path with a valid path
  cat(output, file="Server_Path/EA_AssignmentTest.txt", append=TRUE, sep = ",")
  
  #Garbage collection statement
  gc()
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
  
  time.taken <- as.data.frame(time.taken)
  
  #Output the time taken to a text file
  cat(time.taken, file="D:/Geo XAC/EA assignment project/EAAssignmentTime.txt", append=TRUE, sep = ",")
}

#Copy output file to server
#This path should be the same as the file which is appended at each iteration of the loop above
#Replace Server_Path with a valid path
file.copy("Server_Path/EA_AssignmentTest.txt","//sas-server/S$/Ruan/EA_Assignment",overwrite = TRUE)

#Create new table
sqlQuery(myconn,"create table Ruan..EAAssignmentTest (
         ConsumerID varchar(255),
         ConsumerGeoAddressID varchar(255),
         EA_CODE varchar(255),
		 LastUpdatedDate date)")

#Bulk import file
sqlQuery(myconn,"BULK INSERT Ruan..EAAssignmentTest FROM '\\\\sas-server\\S$\\Ruan\\EA_Assignment\\EA_AssignmentTest.txt' WITH (FIELDTERMINATOR = ',',ROWTERMINATOR = '\\n')")

#Close database connection
close(myconn)

#Delete data no longer needed
rm(list = c("output", "shapedataframe", "shapeids","coordinates","i","myconn","shapes","packages"))
