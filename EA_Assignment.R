#Install necessary packages if not already installed
packages <- c("shapefiles","plyr","ggplot2","rgdal","foreach","RODBC","doParallel")
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
  install.packages(setdiff(packages, rownames(installed.packages())))
}

#READ SHAPE FILES
library(shapefiles)
library(plyr)
library(ggplot2)
library(rgdal)
library(foreach)
library(RODBC)
library(SOAR)
library(doParallel)
library(DBI)

ea_assignment_loop <- function(a,shapes,shapeids,coordinates,output){
  #Select the current shape - Indexed by EA_CODE
  #PR_MDB_C
  shapesubset <- subset(shapes, shapes$EA_CODE == shapeids[a,] & shapes$PR_MDB_C == province)
  
  temp = !is.na(over(coordinates, as(shapesubset, "SpatialPolygons")))
  
  if(sum(temp) > 0){
    
    temp <- as.data.frame(temp)
    
    temp <- temp + 0
    
    temp$ConsumerID <- coordinates@data$ConsumerID
    
    temp$EA_CODE <- coordinates@data$EA_CODE
    
    temp$ConsumerGeoAddressID <- coordinates@data$ConsumerGeoAddressID
    
    temp <- temp[temp$temp > 0,]
    
    #Drop temp column
    temp$temp <- NULL
    
    temp$EA_CODE <- shapeids[a,]
    
    data.frame(ConsumerID = temp$ConsumerID, EA_CODE = temp$EA_CODE, ConsumerGeoAddressID = temp$ConsumerGeoAddressID)
  }
}

#Define a province name in the following list:
#EC
#FS
#GT
#KZN
#LIM
#MP
#NC
#NW
#WC
province = "FS"

#Read in shape files
shapes <- readOGR("E:/Ruan/Census 2011 Spatial Geography","EA_SA_2011")

#SQL connection
#myconn <- dbConnect(RSQLite::SQLite(), dbname = "GeoExport")

#Read entire text file into a SQLite database
#dbWriteTable(myconn, name="GeoExport", value="E:/Ruan/GeoExport.txt",row.names=FALSE,header=TRUE,sep="|",overwrite=TRUE)

#Output EA characteristics to SQL 
#sqlSave(myconn, shapedataframe, tablename = "EASpecification", append = TRUE)

#Function to read in a part of the input data
ea_assignment <- function(a){
  #Build query string
  #Only select addresses that have non-missing coordinates
  if(a < 2){
    #query <- paste("select ConsumerID, LastUpdatedDate, X, Y from ConsumerGeoAddress..GeoExport where ConsumerGeoAddressID < ",a*250000 ,sep="")
    query <- paste("select ConsumerID, X, Y, ConsumerGeoAddressID from ConsumerGeoAddress..ProvinceAssignment where ConsumerGeoAddressID < ",a*1000000," and Province = '",province,"'",sep="")
  }
  else{
    #query <- paste("select ConsumerID, LastUpdatedDate, X, Y from ConsumerGeoAddress..GeoExport where ConsumerGeoAddressID < ",a*250000, " and ConsumerGeoAddressID >= ", (a-1)*250000,sep="")
    query <- paste("select ConsumerID, X, Y, ConsumerGeoAddressID from ConsumerGeoAddress..ProvinceAssignment where ConsumerGeoAddressID < ",a*1000000, " and ConsumerGeoAddressID >= ", (a-1)*1000000," and Province = '",province,"'",sep="")
  }
  
  return (sqlQuery(myconn,query))
}

#Register multi-core backend
registerDoParallel(detectCores())    

#Drop EAAssignment table if it exists
#sqlQuery(myconn, "IF OBJECT_ID('ConsumerGeoAddress..EAAssignment', 'U') IS NOT NULL
#DROP TABLE ConsumerGeoAddress..EAAssignment;")

#Get the total number of rows in the database
#parts <- sqlQuery(myconn, "select count(*) from ConsumerGeoAddress..GeoExport")
parts <- sqlQuery(myconn,paste("select count(*) from ConsumerGeoAddress..ProvinceAssignment where Province = '",province,"'",sep=""))

#Determine how many runs, doing one million rows at a time, we will need to do
numruns <- ceiling(parts/1000000)

for(a in 1:numruns[[1]]){
  start.time <- Sys.time()
  print(a)
  #Get the n-th million of the data
  coordinates <- ea_assignment(a)
  
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
  
  #Rename column to EA_CODE
  colnames(output)[2] <- c("EA_CODE")
  
  #Rename column to ConsumerGeoAddressID
  colnames(output)[3] <- c("ConsumerGeoAddressID")
  
  #Loop through all shapeids and check whether a coordinate is in that shape
  output <- foreach(i=1:nrow(shapeids),.packages=c("plyr"),.combine='rbind') %dopar% { 
    ea_assignment_loop(i,shapes,shapeids,coordinates,output)
  }
  
  #Garbage collection statement
  gc()
  
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
  
  time.taken <- as.data.frame(time.taken)
  
  if(a < 2){
    #Output the time taken to a text file
    write.table(time.taken,file=paste("E:/Ruan/EA Assignment/Time_Taken_",province,".csv",sep=""),append=TRUE,sep=",",row.names=F,quote=F)
    #Output ConsumerID and PR_MDB_C pairs to a text file
    write.table(output,file=paste("E:/Ruan/EA Assignment/EA_Assignment_",province,".csv",sep=""),append=TRUE,sep=",",row.names=F,quote=F)
  }
  else{
    write.table(time.taken,file=paste("E:/Ruan/EA Assignment/Time_Taken_",province,".csv",sep=""),append=TRUE,sep=",",row.names=F,quote=F,col.names=F)
    write.table(output,file=paste("E:/Ruan/EA Assignment/EA_Assignment_",province,".csv",sep=""),append=TRUE,sep=",",row.names=F,quote=F,col.names=F)
  }
}

#Close database connection
close(myconn)

#Delete data no longer needed
rm(list = c("output", "shapedataframe", "shapeids","coordinates","i","myconn","shapes","packages","province","numruns","parts","time.taken","a","end.time","start.time","ea_assignment","ea_assignment_loop"))
