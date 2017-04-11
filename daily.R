library(RTrade)
#library(doParallel)
library(quantmod)
#source("CustomFunctions.R")

setwd("/home/psharma/Dropbox/rfiles/daily")
splits <-
  read.csv("splits.csv", header = TRUE, stringsAsFactors = FALSE)
symbolchange <-
  read.csv("symbolchange.csv",
           header = TRUE,
           stringsAsFactors = FALSE)
symbolchange <-
  data.frame(key = symbolchange$SM_KEY_SYMBOL,
             newsymbol = symbolchange$SM_NEW_SYMBOL)
#symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
#symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)

splits <-
  data.frame(
    date = as.POSIXct(splits$date, tz = "Asia/Kolkata"),
    symbol = splits$symbol,
    oldshares = splits$oldshares,
    newshares = splits$newshares
  )
#splits$symbol = gsub("[^0-9A-Za-z/-]", "", splits$symbol)

# niftysymbols <-
#   read.csv("symbols.csv",
#            header = TRUE,
#            stringsAsFactors = FALSE)

niftysymbols <-readAllSymbols(2,"ibsymbols")
#folots <- createFNOSize(2, "contractsize", threshold = "2005-01-01")
#folots$symbol<-sapply(folots$symbol,getMostRecentSymbol,symbolchange$key,symbolchange$newsymbol)
#folotssymbols = unique(folots$symbol)
# folotsuniquesymbols <-
#   sapply(folotssymbols,
#          getMostRecentSymbol,
#          symbolchange$key,
#          symbolchange$newsymbol)
# niftysymbols$Symbol <-
#   gsub("[^0-9A-Za-z/-]", "", niftysymbols$Symbol) # Keep "-" in symbol name

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

