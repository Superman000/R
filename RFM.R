#Define a function which assigns one RFM codes to each person
RFM <- function(df,startDate,endDate,tIDColName="ID_COL_NAME",tDateColName="DATE_COL_NAME",tAmountColName="AMT_COL_NAME"){
  #order the dataframe by date descendingly
  df <- df[order(df[,tDateColName],decreasing = TRUE),]
  #remove the record before the start data and after the end Date
  df <- df[df[,tDateColName]>= startDate,]
  df <- df[df[,tDateColName]<= endDate,]
  #remove the rows with the duplicated IDs, and assign the df to a new df.
  newdf <- df[!duplicated(df[,tIDColName]),]
  # caculate the Recency(days) to the endDate, the smaller days value means more recent
  Recency<-as.numeric(difftime(endDate,newdf[,tDateColName],units="days"))
  # add the Days column to the newdf data frame
  newdf <-cbind(newdf,Recency)
  #order the dataframe by ID to fit the return order of table() and tapply()
  newdf <- newdf[order(newdf[,tIDColName]),]
  # caculate the frequency
  fre <- as.data.frame(table(df[,tIDColName]))
  Frequency <- fre[,2]
  newdf <- cbind(newdf,Frequency)
  #caculate the Money per deal
  m <- as.data.frame(tapply(df[,tAmountColName],df[,tIDColName],sum))
  Monetary <- m[,1]/Frequency
  newdf <- cbind(newdf,Monetary)
  return(newdf)
}

#Define RFM score function
#Change the numbers next to R, F and M to customise the number of unique codes outputted
GetRFMScore <- function(df,r = 3,f = 3,m = 3) {
  if (r <= 0 || f <= 0 || m <= 0) return
  #order and the score
  df <- df[order(df$Recency,-df$Frequency,-df$Monetary),]
  R_Score <- scoring(df,"Recency",r)
  df <- cbind(df, R_Score)
  df <- df[order(-df$Frequency,df$Recency,-df$Monetary),]
  F_Score <- scoring(df,"Frequency",f)
  df <- cbind(df, F_Score)
  df <- df[order(-df$Monetary,df$Recency,-df$Frequency),]
  M_Score <- scoring(df,"Monetary",m)
  df <- cbind(df, M_Score)
  #order the dataframe by R_Score, F_Score, and M_Score desc
  df <- df[order(-df$R_Score,-df$F_Score,-df$M_Score),]
  # caculate the total score
  Total_Score <- c(100*df$R_Score + 10*df$F_Score+df$M_Score)
  df <- cbind(df,Total_Score)
  return (df)
}

#Define a function invoked by GetRFMScore
scoring <- function (df,column,r){
  #get the length of rows of df
  len <- dim(df)[1]
  score <- rep(0,times=len)
  # get the quantity of rows per 1/r e.g. 1/5
  nr <- round(len / r)
  if (nr > 0){
    # seperate the rows by r aliquots
    rStart <-0
    rEnd <- 0
    for (i in 1:r){
      #set the start row number and end row number
      rStart = rEnd+1
      #skip one "i" if the rStart is already in the i+1 or i+2 or ...scope.
      if (rStart> i*nr) next
      if (i == r){
        if(rStart<=len ) rEnd <- len else next
      }else{
        rEnd <- i*nr
      }
      # set the Recency score
      score[rStart:rEnd]<- r-i+1
      # make sure the customer who have the same recency have the same score
      s <- rEnd+1
      if(i<r & s <= len){
        for(u in s: len){
          if(df[rEnd,column]==df[u,column]){
            score[u]<- r-i+1
            rEnd <- u
          }else{
            break;
          }
        }
      }
    }
  }
  return(score)
}

#Usage example
#Obtain appropriate date range values
min_date <- min(transaction_df$DATE_COL_NAME)
max_date <- max(transaction_df$DATE_COL_NAME)

#Call the RFM function to determine RFM raw values
rfm_df <- RFM(transaction_df,min_date,max_date)

#Obtain RFM scores from the raw result
rfm_df <- RGetRFMScore(rfm_df,min_date,max_date)
