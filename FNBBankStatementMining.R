#Define function used to generate a dataframe from an English FNB cheque account bank statement
BankStatementToPDF <- function(path){
  #Read in PDF documents
  pdf <- pdf_text(path)
  
  #Concatenate text
  pdf <- paste(pdf, collapse = '',sep ='')
  
  #Remove '\r\n' pattern
  pdf <- gsub('\r\n','',pdf)
  
  #Look for the statement date in the PDF text
  statement_date_start <- regexpr("Statement Period",pdf)
  statement_date_end <- regexpr("Statement Date",pdf)
  date <- trimws(substr(pdf,statement_date_start[1],statement_date_end[1]-1))
  
  #Find the from month name
  statement_date_pos <- unlist(gregexpr("([0-9]{4})",date,perl=T))
  
  #Get the statement start and end dates (YYYY)
  start_date <- substr(date,statement_date_pos[1],statement_date_pos[1]+3)
  end_date <- substr(date,statement_date_pos[2],statement_date_pos[2]+3)
  
  #Find the position where the monthly transaction list starts
  start <- regexpr("FNB PREMIER CHEQUE ACCOUNT: 62230996764",pdf)
  
  #Get position where transaction list ends
  end <- regexpr("Please contact us within 30 days from your statement date",pdf)
  
  #Isolate piece of text that contains transaction information
  trans <- substr(pdf,start[[1]],end[[1]]-2)
  
  #Find position of 'Opening Balance phrase'
  pos_opening_balance <- regexpr("Cr",trans)
  
  #Cut off text before transaction information start
  trans <- substr(trans,pos_opening_balance[[1]]+3,nchar(trans))
  
  #Define list of month abbreviations
  month_abbreviations <- c("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
  
  temp <- ""
  
  #Create valid RegEx based on month list
  for(a in 1:length(month_abbreviations)){
    if(a < length(month_abbreviations))
      temp <- paste(temp,"",month_abbreviations[a],"|",sep="")
    else
      temp <- paste(temp,"",month_abbreviations[a],"",sep="")
  }
  
  reg_ex_months <- paste("(?:^|(?<= ))(",temp,")(?:(?= )|$)",sep="")
  
  #Each time we encounter the pattern 'xx MonthAbbr', we know that a new transaction line is starting
  date_pos <- unlist(gregexpr(paste("[0-9]{2}",reg_ex_months,sep=" "),trans,perl=T))
  
  #Add a pipe character after the occurance of each date
  for(b in 1:length(date_pos)){
    if(b != 1 && ((substr(trans,date_pos[b]+6,date_pos[b]+26) != "                     ") && substr(trans,date_pos[b]-6,date_pos[b]-6) != "*"))
      substr(trans,date_pos[b]-1,date_pos[b]+6) <- "|"
  }
  
  trans <- unlist(strsplit(trans,"[|]"))
  
  #Add a pipe character after the occurance of each whitespace sequence
  for(b in 1:length(trans)){
    white_space_indexes <- unlist(gregexpr("[  ]{4,}",trans[b],perl=T))
    for(c in 1:length(white_space_indexes)){
      substr(trans[b],white_space_indexes[c],white_space_indexes[c]+1) <- "|"
    }
  }
  
  #Loop through each row and populate an output dataframe
  #Begin building output data frame
  output <- as.data.frame(matrix(0, ncol = 6, nrow = 0))
  
  #Rename column to Date
  colnames(output)[1] <- c("Date")
  
  #Rename column to Description
  colnames(output)[2] <- c("Description")
  
  #Rename column to Reference
  colnames(output)[3] <- c("Reference")
  
  #Rename column to Amount
  colnames(output)[4] <- c("Amount")
  
  #Rename column to Balance
  colnames(output)[5] <- c("Balance")
  
  #Rename column to AccruedBankCharges
  colnames(output)[6] <- c("AccruedBankCharges")
  
  #We now have a consistent format which enables the assignment of data fields to columns in a data frame
  for(c in 1:length(trans)){
    temp <- unlist(strsplit(trans[c],"[|]"))
    if(length(temp) == 5 && !grepl("Closing Balance",temp[4])){
      output <- rbind(output,data.frame(Date = substr(temp[1],1,6), 
                                        Description = sub("^\\s+", "", substr(temp[2],1,nchar(temp[2]))), 
                                        Reference = sub("^\\s+", "",substr(temp[3],1,nchar(temp[3]))), 
                                        Amount = sub("^\\s+", "",substr(temp[4],1,nchar(temp[4]))), 
                                        Balance = sub("^\\s+", "",substr(temp[5],1,nchar(temp[5]))),
                                        AccruedBankCharges = NA))
    }
    else if(grepl("#Value Added Serv Fees",temp[1])){
      output <- rbind(output,data.frame(Date = substr(temp[1],1,6),
                                        Description = sub("^\\s+", "", substr(temp[1],7,nchar(temp[1]))),
                                        Reference = NA,
                                        Amount = sub("^\\s+", "",substr(temp[2],1,nchar(temp[2]))),
                                        Balance = sub("^\\s+", "",substr(temp[3],1,nchar(temp[3]))),
                                        AccruedBankCharges = sub("^\\s+", "",substr(temp[2],1,nchar(temp[2])))))
    }
    else if(length(temp) == 3 || length(temp) == 4){
      if(grepl("#Statement Fee",temp[1]) || grepl("#Monthly Account Fee",temp[1])){
        output <- rbind(output,data.frame(Date = substr(temp[1],1,6),
                                          Description = sub("^\\s+", "", substr(temp[1],7,nchar(temp[1]))),
                                          Reference = NA,
                                          Amount = sub("^\\s+", "",substr(temp[2],1,nchar(temp[2]))),
                                          Balance = sub("^\\s+", "",substr(temp[3],1,nchar(temp[3]))),
                                          AccruedBankCharges = NA))
      }
      else{
        output <- rbind(output,data.frame(Date = substr(temp[1],1,6),
                                          Description = sub("^\\s+", "", substr(temp[2],1,nchar(temp[2]))),
                                          Reference = NA,
                                          Amount = sub("^\\s+", "",substr(temp[3],1,nchar(temp[3]))),
                                          Balance = sub("^\\s+", "",substr(temp[4],1,nchar(temp[4]))),
                                          AccruedBankCharges = NA))
      }
    }
  }
  
  #Rework abbreviated month names to be displayed as a number instead
  output$DateMonth <- substr(output$Date,4,8)
  output$MonthNumber <- ifelse(output$DateMonth == "Jan",1,
                               ifelse(output$DateMonth == "Feb",2,
                                      ifelse(output$DateMonth == "Mar",3,
                                             ifelse(output$DateMonth == "Apr",4,
                                                    ifelse(output$DateMonth == "May",5,
                                                           ifelse(output$DateMonth == "Jun",6,
                                                                  ifelse(output$DateMonth == "Jul",7,
                                                                         ifelse(output$DateMonth == "Aug",8,
                                                                                ifelse(output$DateMonth == "Sep",9,
                                                                                       ifelse(output$DateMonth == "Oct",10,
                                                                                              ifelse(output$DateMonth == "Nov",11,
                                                                                                     ifelse(output$DateMonth == "Dec",12,NA))))))))))))
  
  #Strip out day number
  output$Day <- substr(output$Date,1,2)
  
  #Build proper date variable
  output$Date <- dmy(paste(output$Day,output$DateMonth,start_date,sep="/"))
  
  #Drop temporary variables
  output$Day <- NULL
  output$DateMonth <- NULL
  output$MonthNumber <- NULL
  
  #If balance contains a 'Cr', leave as is
  #If balance contains a 'Dr', make negative
  #First create indicator for the presence of a 'Cr'
  #Convert Balance to a character type
  output$Balance <- as.character(output$Balance)
  output$BalanceInd <- ifelse(grepl("Cr",output$Balance),1,0)
  
  #Strip out 'Cr' and 'Dr'
  output$Balance <- substr(output$Balance,1,nchar(output$Balance)-3)
  
  #Strip out thousands separator from Balance and make numeric
  output$Balance <- as.numeric(gsub(",","",output$Balance))
  
  #Multiply by -1 if BalanceInd = 0
  output$Balance <- ifelse(output$BalanceInd == 0,output$Balance*-1,output$Balance)
  
  #Drop BalanceInd
  output$BalanceInd <- NULL
  
  #Do the same for the Amount column
  output$Amount <- as.character(output$Amount)
  output$BalanceInd <- ifelse(grepl("Cr",output$Amount),1,0)
  
  #Strip out 'Cr' from Amount
  output$Amount <- ifelse(output$BalanceInd == 1,substr(output$Amount,1,nchar(output$Amount)-3),output$Amount)
  
  #Strip out thousands separator from Amount and make numeric
  output$Amount <- as.numeric(gsub(",","",output$Amount))
  
  #Multiply by -1 if BalanceInd = 0
  output$Amount <- ifelse(output$BalanceInd == 0,output$Amount*-1,output$Amount)
  
  #Drop BalanceInd
  output$BalanceInd <- NULL
  
  return(output)
}

#Assign libraries
library(pdftools)
library(lubridate)
library(sqldf)

#Get paths for all PDF documents in a folder
path <- "C:/Users/rvanzyl/Documents/XDS/FNB PDF Mining"
files <- list.files(path=path,pattern="\\.pdf$")

#Loop through paths and generate data frames
for(i in 1:length(files)){
  assign(paste("FNB",substr(files[i],nchar(files[i])-5,nchar(files[i])-4),sep="_"),BankStatementToPDF(paste(path,"/",files[i],sep="")))
}

#Vertically combine generated dataframes to create a master set
master <- rbind(XX,YY,ZZ)

#Produce plot of balance over time
plot(master$Date,master$Balance,xlab="Date",ylab="Balance",type="l")
