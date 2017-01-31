#Get list of directories (folders) within a directory
folders <- list.dirs(path = "D:/CYL2289 - D&S Forensics - ICC - Results/All Data Exports/Tjaart Dell LPT/All Mail", full.names = TRUE, recursive = TRUE)

#Loop through those directories
#I wanted to exclude the first two folders
for(i in 3:length(folders)){
  #Get all the files in that folder
  #It is also possible to limit the files to those having a specific extension
  files <- list.files(path=folders[i])
  #Loop through the files
  for(a in 1:length(files)){
    #Copy the files to a different (fixed) location
    file.copy(paste(folders[i],"/",files[a],sep=""),paste("C:/Users/rvanzyl/Desktop/Email export/",files[a],sep=""))
  }
}
