#Assign libraries
library(ggmap)
library(RODBC)

#Create a matrix in which the geo coding result will be stored
geo_coding_output <- matrix(NA,nrow=nrow(address_sample),ncol=3)

#Geo code the addresses in a loop
for(i in 1:nrow(address_sample)){
  #Get the object corresponding to the current iteration in the loop
  curr <- address_sample[i,]
  geo_reply <- geocode(as.character(curr$OriginalAddress1),output='all',messaging=TRUE,override_limit=TRUE)
  
  #Check if the geo coding was successful
  if(geo_reply$status == "OK"){
    geo_coding_output[i,1] <- curr$ID
    geo_coding_output[i,2] <- geo_reply$results[[1]]$geometry$location$lat
    geo_coding_output[i,3] <- geo_reply$results[[1]]$geometry$location$lng 
  }
  #If unsuccessful, assign NAs to that ID
  else{
    geo_coding_output[i,1] <- curr$ID
    geo_coding_output[i,2] <- NA
    geo_coding_output[i,3] <- NA
  }
}
