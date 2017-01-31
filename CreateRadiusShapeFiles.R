#Assign libraries
library(sp)
library(rjson)
library(RJSONIO)
library(rgdal)
library(ggplot2)
library(ggmap)
library(rworldmap)
library(geosphere)
library(sp)
library(spatstat)
library(dismo)
library(rgeos)
library(shapefiles)
library(rgdal)
library(RODBC)
library(SOAR)
library(sqldf)
library(proj4)
library(maptools)

#Create sample data set
#When we do this for all stores, a CSV file would typically be read at this point
store_data <- data.frame(StoreCode = 1111,LocationDesc = "Rosewood Square - Kokstad", Lat = -30.509628, Lon = 29.406323)
store_data <- rbind(store_data,data.frame(StoreCode = 2222, LocationDesc = "Park Station - Johannesburg", Lat = -26.197209, Lon = 28.04218))

#Define radius object
#This means that we will draw circle shape files around the coordinates which have 5, 10, 20 and 50 km radii
rad <- list(5,10,20,50)

#Initialise necessary objects
newmap <- getMap(resolution = "low")
temp_map <- gmap("South Africa",zoom=1,scale=1)

for(i in 1:nrow(store_data)){
  #Define temporary data frame containing the lat lon conbination of a store
  d <- data.frame(lat=as.numeric(as.character(store_data$Lat[i])),lon=as.numeric(as.character(store_data$Lon[i])))
  
  #Covert to coordinate data type and set projection
  coordinates(d) <- ~ lon + lat
  proj4string(d) <- projection(newmap)
  
  #Convert projection to be the same as that of the Google Maps object
  d_mrc <- spTransform(d, CRS = CRS(projection(temp_map)))
  
  #Repeat the process for each radius
  for(q in 1:length(rad)){
    #Buffer creation - Width = Radius in meters
    d_mrc_bff <- gBuffer(d_mrc,width=as.numeric(rad[[q]])*1000)
    
    #Convert a spatialpolygons object to a spatialpolygonsdataframe object
    df <- data.frame(id = getSpPPolygonsIDSlots(d_mrc_bff))
    row.names(df) <- getSpPPolygonsIDSlots(d_mrc_bff)
    
    #Make spatial polygon data frame
    #This is the shape object of the drawn radius around the store coordinate
    spdf <- SpatialPolygonsDataFrame(d_mrc_bff,data=df)
    
    #Make sure the new object has the correct projection
    spdf <- spTransform(spdf, CRS(projection(newmap)))

    #Write shape file to output file
    #File name format: StoreCode_Radius
    writeOGR(spdf, paste("C:/Users/rvanzyl/Documents/XDS/Cell C Geo Project/Data/",store_data$StoreCode[i],"_",rad[[q]],".shp",sep=""), "OGRGeoJSON",driver="ESRI Shapefile")
  }
}
