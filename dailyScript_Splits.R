library(rredis)
library(RCurl)
library(stringi)


setwd("/home/psharma/Dropbox/rfiles/daily")
splits <-  read.csv("splits.csv", header = TRUE, stringsAsFactors = FALSE)
redisConnect()
redisSelect(2)
for(i in 1:nrow(splits)){
  redisString = paste(strftime(splits$date[i],"%Y%m%d"),splits$symbol[i],splits$oldshares[i],splits$newshares[i],splits$X[i], sep = "_")
  redisSAdd("splits",charToRaw(redisString))
}

myOpts<-curlOptions(
  referer="https://www1.nseindia.com/corporates/corporateHome.html",
  verbose = TRUE, 
  followLocation = TRUE,
  useragent = "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13"
)
jfile <- getURL("https://www1.nseindia.com/corporates/datafiles/CA_LAST_12_MONTHS_BONUS.csv",
                .opts=myOpts)
writeChar(jfile, paste("splits/bonus_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""))
splitsnew <-  read.csv(paste("splits/bonus_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
purpose<-trimws(splitsnew$Purpose)
purpose<-gsub(" ","",splitsnew$Purpose,fixed=TRUE)
purpose<-strsplit(purpose, 'Bonus')
bonus<-sapply(purpose,function(x) x[2])
splitratio<-strsplit(bonus,":")
newshares<-sapply(splitratio,function(x) x[1])
oldshares<- sapply(splitratio,function(x) x[2])
oldshares<-strsplit(oldshares,"/")
oldshares<-lapply(oldshares,function(x) x[1])
oldshares<-as.numeric(oldshares)
newshares<-as.numeric(newshares)
newshares<-newshares+oldshares
bonusdate<-as.Date(splitsnew$Ex.Date,format="%d-%b-%Y",tz="Asia/Kolkata")
bonusdate<-strftime(bonusdate,"%Y%m%d")
symbol<-trimws(splitsnew$Symbol)
for(i in 1:length(symbol)){
  redisString = paste(bonusdate[i],symbol[i],oldshares[i],newshares[i],"Bonus", sep = "_")
  redisSAdd("splits",charToRaw(redisString))
}


# Get forthcoming bonus
jfile <- getURL("https://www1.nseindia.com/corporates/datafiles/CA_ALL_FORTHCOMING_BONUS.csv",
                .opts=myOpts)
writeChar(jfile, paste("splits/forthcomingbonus_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""))
splitsnew <-  read.csv(paste("splits/forthcomingbonus_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
if(nrow(splitsnew)>0){
  purpose<-trimws(splitsnew$Purpose)
  purpose<-gsub(" ","",splitsnew$Purpose,fixed=TRUE)
  purpose<-strsplit(purpose, 'Bonus')
  bonus<-sapply(purpose,function(x) x[2])
  splitratio<-strsplit(bonus,":")
  newshares<-sapply(splitratio,function(x) x[1])
  oldshares<- sapply(splitratio,function(x) x[2])
  oldshares<-strsplit(oldshares,"/")
  oldshares<-lapply(oldshares,function(x) x[1])
  oldshares<-as.numeric(oldshares)
  newshares<-as.numeric(newshares)
  newshares<-newshares+oldshares
  bonusdate<-as.Date(splitsnew$Ex.Date,format="%d-%b-%Y",tz="Asia/Kolkata")
  bonusdate<-strftime(bonusdate,"%Y%m%d")
  symbol<-trimws(splitsnew$Symbol)
  for(i in 1:length(symbol)){
    redisString = paste(bonusdate[i],symbol[i],oldshares[i],newshares[i],"Bonus", sep = "_")
    redisSAdd("splits",charToRaw(redisString))
  }
}
# Get prior splits
  jfile <- getURL("https://www1.nseindia.com/corporates/datafiles/CA_LAST_12_MONTHS_SPLIT.csv",
                  .opts=myOpts)
  writeChar(jfile, paste("splits/split_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""))
  splitsnew <-  read.csv(paste("splits/split_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
  if(nrow(splitsnew)>0){
    purpose<-trimws(splitsnew$Purpose)
    purpose<-gsub(" ","",splitsnew$Purpose,fixed=TRUE)
    purpose<-strsplit(purpose, 'Bonus')
    purpose<-sapply(purpose,function(x) x[length(x)])
    purpose<-strsplit(purpose, 'FaceValue')
    purpose<-sapply(purpose,function(x) x[length(x)])
    purpose<-strsplit(purpose, 'FvSplt')
    purpose<-sapply(purpose,function(x) x[length(x)])
    newshares<-stri_extract_first_regex(purpose, "[0-9]+")
    oldshares<-stri_extract_last_regex(purpose, "[0-9]+")
    newshares<-as.numeric(newshares)
    oldshares<-as.numeric(oldshares)
    bonusdate<-as.Date(splitsnew$Ex.Date,format="%d-%b-%Y",tz="Asia/Kolkata")
    bonusdate<-strftime(bonusdate,"%Y%m%d")
    symbol<-trimws(splitsnew$Symbol)
    for(i in 1:length(symbol)){
      redisString = paste(bonusdate[i],symbol[i],oldshares[i],newshares[i],"Split", sep = "_")
      redisSAdd("splits",charToRaw(redisString))
    }
  }
    
  # Get forthcoming splits
  jfile <- getURL("https://www1.nseindia.com/corporates/datafiles/CA_ALL_FORTHCOMING_SPLIT.csv",
                  .opts=myOpts)
  writeChar(jfile, paste("splits/forthcomingsplit_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""))
  splitsnew <-  read.csv(paste("splits/forthcomingsplit_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
  if(nrow(splitsnew)>0){
    purpose<-trimws(splitsnew$Purpose)
    purpose<-gsub(" ","",splitsnew$Purpose,fixed=TRUE)
    purpose<-strsplit(purpose, 'Bonus')
    purpose<-sapply(purpose,function(x) x[length(x)])
    purpose<-strsplit(purpose, 'FaceValue')
    purpose<-sapply(purpose,function(x) x[length(x)])
    purpose<-strsplit(purpose, 'FvSplt')
    purpose<-sapply(purpose,function(x) x[length(x)])
    newshares<-stri_extract_first_regex(purpose, "[0-9]+")
    oldshares<-stri_extract_last_regex(purpose, "[0-9]+")
    newshares<-as.numeric(newshares)
    oldshares<-as.numeric(oldshares)
    bonusdate<-as.Date(splitsnew$Ex.Date,format="%d-%b-%Y",tz="Asia/Kolkata")
    bonusdate<-strftime(bonusdate,"%Y%m%d")
    symbol<-trimws(splitsnew$Symbol)
    for(i in 1:length(symbol)){
      redisString = paste(bonusdate[i],symbol[i],oldshares[i],newshares[i],"Split", sep = "_")
      redisSAdd("splits",charToRaw(redisString))
    }
  }  
    
a<-unlist(redisSMembers("splits")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
dat <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
  for(j in 1:k[i]){
    runsum=cumsum(k)[i]
    dat[i, j] <- allvalues[runsum-k[i]+j]
  }
}
dat$date=strptime(dat$date,format="%Y%m%d",tz="Asia/Kolkata")
write.csv(dat,file="splits/summary.csv")
redisClose()

#save changes in symbol names
redisConnect()
redisSelect(2)

jfile <- getURL("https://www1.nseindia.com/content/equities/symbolchange.csv",.opts=myOpts)
writeChar(jfile, paste("splits/symbolchange_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""))
symbolchange <-  read.csv(paste("splits/symbolchange_",strftime(Sys.Date(),"%Y%m%d"),".csv",sep=""), header = TRUE, stringsAsFactors = FALSE)
symbolchange$SM_APPLICABLE_FROM<-as.Date(symbolchange$SM_APPLICABLE_FROM,format="%d-%b-%Y",tz="Asia/Kolkata")
symbolchange$SM_APPLICABLE_FROM<-strftime(symbolchange$SM_APPLICABLE_FROM,"%Y%m%d")
for(i in 1:nrow(symbolchange)){
  redisString = paste(symbolchange$SM_APPLICABLE_FROM[i],symbolchange$SM_KEY_SYMBOL[i],symbolchange$SM_NEW_SYMBOL[i], sep = "_")
  redisSAdd("symbolchange",charToRaw(redisString))
}

