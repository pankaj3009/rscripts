library(RTrade)
#library(doParallel)
library(quantmod)

setwd("/home/psharma/Dropbox/rfiles/daily")
redisConnect()
redisSelect(2)
#update splits
a<-unlist(redisSMembers("splits")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
splits <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
  for(j in 1:k[i]){
    runsum=cumsum(k)[i]
    splits[i, j] <- allvalues[runsum-k[i]+j]
  }
}
splits$date=as.POSIXct(splits$date,format="%Y%m%d",tz="Asia/Kolkata")
splits$oldshares<-as.numeric(splits$oldshares)
splits$newshares<-as.numeric(splits$newshares)

#update symbol change
a<-unlist(redisSMembers("symbolchange")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
symbolchange <- data.frame(date=rep("",length(a)), key=rep("",length(a)),newsymbol=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
  for(j in 1:k[i]){
    runsum=cumsum(k)[i]
    symbolchange[i, j] <- allvalues[runsum-k[i]+j]
  }
}
symbolchange$date=as.POSIXct(symbolchange$date,format="%Y%m%d",tz="Asia/Kolkata")
symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
redisClose()

niftysymbols <-readAllSymbols(2,"ibsymbols")

endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
#cl <- makeCluster(detectCores())
#cl <- makeCluster(4)
#registerDoParallel(cl)

for (i in 1:nrow(niftysymbols)) {
#  foreach (i =1:nrow(niftysymbols),.packages="RTrade") %dopar% {
  print(i)
  md = data.frame() # create placeholder for market data
  symbol = niftysymbols$exchangesymbol[i]
  #symbol = folots$symbol[i]
  if (length(grep("CNX", symbol)) == 0 ||
      length(grep("NSENIFTY", symbol)) == 0) {
    #kairossymbol=gsub("[^0-9A-Za-z///' ]","",symbol)
    kairossymbol = symbol
    if (file.exists(paste("../daily/", symbol, ".Rdata", sep = ""))) {
      load(paste("../daily/", symbol, ".Rdata", sep = ""))
      if (nrow(md) > 0) {
        starttime = strftime(md[nrow(md), c("date")] + 1,
                             tz = "Asia/Kolkata",
                             "%Y-%m-%d %H:%M:%S")
      }
    } else{
      starttime = "1995-11-01 09:15:00"
    }
    temp <-
      kGetOHLCV(
        paste("symbol", tolower(kairossymbol), sep = "="),
        df = md,
        start = starttime,
        end = endtime,
        timezone = "Asia/Kolkata",
        name = "india.nse.equity.s4.daily",
        ts = c(
          "open",
          "high",
          "low",
          "settle",
          "close",
          "volume",
          "delivered"
        ),
        aggregators = c("first",
                        "max",
                        "min",
                        "last",
                        "last",
                        "sum",
                        "sum"),
        aValue = "1",
        aUnit = "days",
        splits = splits,
        symbolchange = symbolchange
        
      )
    #  md <- rbind(md, temp)
    md <- temp
    if (nrow(md) > 0) {
      #md$symbol<-symbol
      save(md,
           file = paste("../daily/", symbol, ".Rdata", sep = "")) # save new market data to disc
      
    }
  }
}
#stopCluster(cl)

kairossymbol = "NSENIFTY"
endtime = format(Sys.time(), format = "%Y-%m-%d %H:%M:%S")
md = data.frame() # create placeholder for market data
if (file.exists(paste(kairossymbol, ".Rdata", sep = ""))) {
  #Load image if it exists to save time
  load(paste(kairossymbol, ".Rdata", sep = ""))
  start = strftime(md[nrow(md), c("date")] + 1, tz = "Asia/Kolkata", "%Y-%m-%d %H:%M:%S")
} else{
  # if image does not exist, give a fixed time
  start = "1995-01-01 09:15:00"
}

  temp <-
    kGetOHLCV(
      paste("symbol", tolower(kairossymbol), sep = "="),
      df=md,
      start = start,
      end = endtime,
      timezone = "Asia/Kolkata",
      name = "india.nse.index.s4.daily",
      ts = c("open", "high", "low","close","settle", "volume"),
      aggregators = c("first", "max", "min","last", "last", "sum"),
      aValue = "1",
      aUnit = "days",
      splits = data.frame(
        date = as.POSIXct(character(), tz = "Asia/Kolkata"),
        symbol = character(),
        oldshares = numeric(),
        newshares = numeric()
      )
    )
  if (nrow(temp) > 0) {
    # change col name of settle to close, if temp is returned with data
    #colnames(temp) <- c("date", "open", "high", "low", "close", "volume","symbol")
    temp$symbol = kairossymbol
  }
  #md <- rbind(md, temp)
  md<-temp
  save(md, file = paste(kairossymbol, ".Rdata", sep = "")) # save new market data to disc
  

# Get niftysymbols$exchangesymbol that does not have data within [R]
niftysymbols$exchangesymbol[!niftysymbols$exchangesymbol %in%gsub("\\.Rdata$","", list.files(pattern="\\.Rdata$"))]

