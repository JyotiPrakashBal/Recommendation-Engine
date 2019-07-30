pages<- read.csv("pages (gor_web).csv") 

admin_ord <- read.csv("admin_cdsource.csv")
ord_comp <- read.csv("order_comp.csv")

ord_comp<- subset(ord_comp,ord_comp$user_id!="")
names(pages)[names(pages)=="User.Id"] <- "user_id"

mis <- read.csv("MIS all cities.csv")

names(mapped2)[names(mapped2)=="Context.Page.Title"] <- "context_page_title"
names(mapped2)[names(mapped2)=="Context.Page.Url"] <- "context_page_url"
names(mapped2)[names(mapped2)=="Context.Page.Path"] <- "context_page_path"
names(mapped2)[names(mapped2)=="Context.Page.Referrer"] <- "context_page_referrer"
names(mapped2)[names(mapped2)=="Context.User.Agent"] <- "context_user_agent"
names(mapped2)[names(mapped2)=="payment_via"] <- "payment_method"
names(mapped2)[names(mapped2)=="Anonymous.Id"] <- "anonymous_id"
names(mapped2)[names(mapped2)=="Context.Ip"] <- "context_ip"
names(mapped2)[names(mapped2)=="gor_id"] <- "order_id"

names(mapped)[names(mapped)=="Context.Page.Path"] <- "context_page_path"
library(dplyr)
df <- bind_rows(mapped,mapped2)

df_not <- subset(pages,pages$user_id=="")
df1 <- df[1:10,]
df2 <- df[10000:10010,]
df3 <- df[50000:50010,]
df4 <- df[100000:100010,]
df5 <- df_not[1:10,]

final= rbind(df,df2,df3,df4)

final <- read.csv("final.csv")
c3 <- read.csv("df_not.csv")
names(c3)[names(c3)=="Context.Ip"] <- "context_ip"
names(c3)[names(c3)=="Context.Page.Referrer"] <- "context_page_referrer"
names(c3)[names(c3)=="Context.Page.Title"] <- "context_page_title"
names(c3)[names(c3)=="Context.Page.Url"]  <- "context_page_url"
names(c3)[names(c3)=="Context.User.Agent"] <- "context_user_agent"
names(c3)[names(c3)=="Anonymous.Id"] <- "anonymous_id"
names(c3)[names(c3)=="Context.Page.Referrer"] <- "context_page_referrer"
c3$Referrer <- NULL
c3$Original.Timestamp <- NULL
c3$Url <- NULL
c3$Title <- NULL
# write.csv(mapped,"op.csv",row.names = FALSE)
# write.csv(mapped2,"ap.csv",row.names = FALSE)

mis <- read.csv("MIS all cities.csv")
cols<- c("Gor.Id","Customer.Name","Customer.Email","Customer.Phone","Quantity","Duration","Main.Category","Sub.category","City","Customer.address","Title")
mis <- mis[,cols]
names(mis)[names(mis)=="Gor.Id"] <- "order_id"

merged <- merge(final,mis,by= "order_id",all.x=TRUE)
merged <- subset(merged,merged$order_id!="GRH36186471")

u1 <-read.csv("u7.csv")
u2 <-read.csv("u8.csv")
u3 <- read.csv("u14.csv")
u4 <- read.csv("u10.csv")
u5 <- read.csv("u16.csv")
#u6 <- read.csv("u6.csv")

final <- rbind(u1,u2,u3,u4,u5)

p1 <-read.csv("p1.csv")
p2 <-read.csv("p2.csv")
p3 <- read.csv("p3.csv")
p4 <- read.csv("p4.csv")
p5 <- read.csv("p5.csv") 

prod_view <- rbind(p1,p2,p3,p4,p5)
write.csv(prod_view,"prd_viewed.csv",row.names = FALSE)
prod_view <- read.csv("prd_viewed.csv")

final$id <- NULL
final$context_campaign_adgroup <- NULL
final$context_campaign_cam1paign <- NULL
final$context_campaign_content <- NULL
final$context_campaign_content <- NULL
final$context_campaign_medium <- NULL
final$context_campaign_name <- NULL
final$context_campaign_source <- NULL
final$context_campaign_term <- NULL
final$context_library_name <- NULL
final$context_library_version <- NULL
final$context_user_agent <- NULL
final$original_timestamp <- NULL
final$received_at <- NULL
final$referrer <- NULL
final$sent_at <- NULL
final$timestamp <- NULL
final$title <- NULL
final$url <- NULL
final$path <- NULL
final$uuid_ts <- NULL

#counting no of vgids for each userid

library(dplyr)
views <- prod_view %>%
  group_by(user_id,product_id) %>%
  summarize(no=n())

prd <- merge(views,prod_view,by="product_id",all.x=TRUE)
names(prd)[names(prd)=="no"] <- "times_viewed"
prd$user_id.y <- NULL

library(lubridate)
library(stringr)
prd$loaded_at <- as.character(prd$loaded_at)
prd$date <- str_sub(prd$loaded_at,1,10)
prd$time <- str_sub(prd$loaded_at,12,19)

prd$date= as.Date(prd$date)

library(tidyr)
prd<- unite(prd, "timestamp", c("date","time"),sep=" ")
prd$timestamp <- as.POSIXct(prd$timestamp)

prd$loaded_at <- NULL

prd <- prd %>% 
  arrange(user_id.x,timestamp)

write.csv(prd,"prd.csv",row.names = FALSE)
prd <- read.csv("prd.csv")

library(dplyr)
prd <- prd %>% 
  arrange(user_id,timestamp)

price_ban <- read.csv("ban.csv")
price_hyd <- read.csv("hyd.csv")
price_gur <- read.csv("gur.csv")
price_mum <- read.csv("mum.csv")

price_all <- rbind(price_ban,price_hyd,price_gur,price_mum)
names(price_all)[names(price_all)=="VGID"] <- "product_id"



price_all <- price_all[!duplicated(price_all$product_id),]

price_merged <- merge(prd,price_all,by="product_id",all.x=TRUE)

prd$date <- as.Date(prd$timestamp,format="%d/%m/%Y %H:%M")

prd <- prd %>% 
  group_by(user_id,product_id) %>%
  arrange(product_id,date) %>%
  mutate(time_lag=lag(date)) %>%
  mutate(diff= date-time_lag) %>%
  mutate(diff= ifelse(is.na(diff),0,diff)) %>%
  mutate(avg= mean(diff))

prd$time_lag <- NULL
prd$category <- as.character(prd$category)

for(i in 1:nrow(prd))
{
  Split <- strsplit(prd$category[i], "/")
  prd$item[i]= Split[[1]][length(Split[[1]])]
}

prd$category <- NULL
prd$diff <- NULL


prod_samp <- prd %>% select(user_id,item)

write.csv(prod_samp,"prod_samp.csv")
user <- unique(prod_samp$user_id)
user <- user[order(user)]
target_users <- user
sno <- 1:length(target_users)
user_ident <- cbind(target_users,sno)

#Create a reference mapper for all merchant
colnames(user_ident) <- c("individual_user","sno")

correlation_mat = matrix(0,length(user),length(target_users))
correlation_mat = as.data.frame(correlation_mat)

trans = read.transactions("prod_samp.csv", format = "single", sep = ",", cols =c("user_id", "item"))

library(descr)
c <- crossTable(trans)
c <- CrossTable(prod_samp$user_id, prod_samp$item,chisq = FALSE, prop.chisq = FALSE)

cd <- data.frame(c)

cccd <- crossTable(cd)

library(caret)
library(dplyr)
cols=c("y","x")

library(Matrix)

data.sparse = sparseMatrix(as.integer(cd$x), as.integer(cd$y), x = cd$Freq)
colnames(data.sparse) = levels(cd$y)
rownames(data.sparse) = levels(cd$x)

train <- as.data.frame(as.matrix(data.sparse))

library("recommenderlab")
affinity.data<-train
affinity.matrix<- as(affinity.data,"realRatingMatrix")

Rec.model<-Recommender(affinity.matrix, method = "IBCF")


as(train, "matrix")
head(as(train, "data.frame"))
library(textir)
library(caret)
library(RANN)
library(arules)

train_m<-preProcess(train,method=c("center","scale"))
train_m<-predict(train_m,newdata=train)

image(as(train,"matrix"), main = "Raw Ratings")       
image(as(train_m,"matrix"), main = "Normalized Ratings")

breaks = c(-Inf,quantile(train[,paste(i,1,sep=".")], na.rm=T),Inf)
breaks = breaks + seq_along(breaks) * .Machine$double.eps

train1<- as(train, "transactions")

write.csv(train,"train1.csv")
train<- read.csv("train.csv")

train1 = read.transactions("prod_samp.csv", format = "single", sep = ",", cols =c("user_id", "item"))
                                                     
train2 <- as(train1, "binaryRatingMatrix")

rec1=Recommender(train2[1:nrow(train2)],method="UBCF", param=list(normalize = "Z-score",method="Cosine",nn=5, minRating=1))
#rec1=Recommender(train2[1:nrow(train2)],method="UBCF", param=list(normalize = "Z-score",method="Jaccard",nn=5, minRating=1))
#rec1=Recommender(train2[1:nrow(train2)],method="IBCF", param=list(method="Jaccard",minRating=1))
# rec=Recommender(r[1:nrow(r)],method="POPULAR")
rec <- Recommender(train2, method = "POPULAR")
pre_3 <- predict(rec, 1:3,data = train2, n = 3)

pppp <- predict(rec, train[1:3], type="ratings")

ppp<- as(pre_3, "list")

pre <- predict(rec, trai, n = 10)

print(rec)
names(getModel(rec))
getModel(rec)$nn

recom <- predict(rec, affinity.matrix, type="ratings")
pre <- predict(rec, train[1:2,] , data = train, n = 3)


# dat <- dummy.data.frame(cd[,cols])
# xyz<- predict(dummyVars(~ ., data = mutate_each(cd[,cols], funs(as.factor))), newdata = cd)
# 
# 
# user <- unique(prd$user_id)
# user <- user[order(user)]
# target_users <- user
# sno <- 1:length(target_users)
# user_ident <- cbind(target_users,sno)
# 
# #Create a reference mapper for all user
# colnames(user_ident) <- c("individual_user","sno")
# 
# correlation_mat = matrix(0,length(users),length(target_users))
# correlation_mat = as.data.frame(correlation_mat)
# 
# trans <- prd %>% select(user_id,item)
# trans <- trans %>% arrange(user_id)
# 
# c <- CrossTable(trans)
