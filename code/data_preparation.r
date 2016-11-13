library(data.table)
library(feather)

path <- "/Users/javier/kaggle/weremeerkat/data/raw/"
clicks.train <- fread(paste0(path,"clicks_train.csv"))
events       <- fread(paste0(path, "events.csv"))
train.events <- merge(clicks.train, events, by = "display_id")
rm(clicks.train,events)

promoted.content <- fread(paste0(path, "promoted_content.csv"))
train.events     <- merge(train.events, promoted.content, by = "ad_id")
setnames(train.events, c("ad_id","display_id","clicked","uuid",
                         "event_document_id","timestamp","platform",
                         "geo_location","promo_document_id","campaign_id",
                         "advertiser_id"))
rm(promoted.content)

## -----------------------------------------------------------------------------
## CATEGORY
## -----------------------------------------------------------------------------
doc.categories <- fread(paste0(path, "documents_categories.csv"))
doc.categories <- doc.categories[doc.categories[, .I[which.max(confidence_level)],
                                                by=list(document_id)]$V1]
train.events   <- merge(train.events, doc.categories, by.x = "event_document_id",
                       by.y = "document_id", all.x = TRUE)
train.events   <- train.events[,-c("confidence_level"), with = FALSE]
rm(doc.categories)
## impute NA with most common category
most.common.category <- as.integer(names(sort(table(train.events$category_id),
                                              decreasing=TRUE))[1])
train.events[is.na(category_id)][["category_id"]] <- most.common.category

## -----------------------------------------------------------------------------
## ENTITY
## -----------------------------------------------------------------------------
doc.entities           <- fread(paste0(path, "documents_entities.csv"))
doc.entities$entity_id <- as.numeric(as.factor(doc.entities$entity_id))
doc.entities           <- doc.entities[order(document_id, -confidence_level)]
## find the top two entities per doc id
tmpdf            <- doc.entities[,.(ent_l = list(entity_id)), by = document_id]
tmpdf$entity_id1 <- unlist(lapply(tmpdf$ent_l, function(x) x[[1]]))
tmpdf$entity_id2 <- unlist(lapply(tmpdf$ent_l,
                                  function(x) ifelse(as.character(x[2])=="NULL",
                                                     NA, x[2])))
tmpdf <- tmpdf[,c("document_id","entity_id1","entity_id2"), with =F]
##  On tmpdf, impute NA with most common entity
most.common.entity <- as.numeric(names(sort(table(tmpdf$entity_id2), decreasing=TRUE)[1]))
tmpdf[is.na(entity_id2)][["entity_id2"]] <- most.common.entity
train.events <- merge(train.events, tmpdf, by.x = "event_document_id",
                      by.y = "document_id", all.x = TRUE)
rm(tmpdf)

## On train events, given the amount of missing doc_ids, inputation strategy is
## a random mix of top 10 entity_ids
set.seed(1981)
length.rep       <- sum(is.na(train.events$entity_id1))
top10.entity_id1 <- as.integer(names(sort(table(train.events$entity_id1),
                                          decreasing = TRUE)[1:10]))
replace.entity_id1 <- sample(top10.entity_id1, length.rep , replace = TRUE)
top10.entity_id2   <- as.integer(names(sort(table(train.events$entity_id2),
                                          decreasing = TRUE)[1:10]))
replace.entity_id2 <- sample(top10.entity_id2, length.rep , replace = TRUE)
tmp.train.df1 <- train.events[is.na(entity_id1)]
tmp.train.df2 <- train.events[!is.na(entity_id1)]
tmp.train.df1$entity_id1 <- replace.entity_id1
tmp.train.df1$entity_id2 <- replace.entity_id2
train.events <- rbindlist(list(tmp.train.df1,tmp.train.df2))
train.events <- train.events[order(display_id)]
rm(tmp.train.df1,tmp.train.df2)

## -----------------------------------------------------------------------------
## META
## -----------------------------------------------------------------------------
doc.meta     <- fread(paste0(path, "documents_meta.csv"))
train.events <- merge(train.events, doc.meta, by.x = "event_document_id",
                      by.y = "document_id", all.x = TRUE)
train.events <- train.events[, dayofweek := weekdays(as.Date(publish_time))]
train.events <- train.events[, timeofday := time.of.day(publish_time)]
train.events <- train.events[,-c("publish_time"), with = FALSE]

## -----------------------------------------------------------------------------
## TOPICS
## -----------------------------------------------------------------------------
doc.topics <- fread(paste0(path , "documents_topics.csv"))
doc.topics <- doc.topics[order(document_id, -confidence_level)]
tmpdf      <- doc.topics[,.(ent_l = list(topic_id)), by = document_id]
tmpdf$topic_id1 <- unlist(lapply(tmpdf$ent_l, function(x) x[[1]]))
tmpdf$topic_id2 <- unlist(lapply(tmpdf$ent_l,
                                  function(x) ifelse(as.character(x[2])=="NULL",
                                                     NA, x[2])))
tmpdf <- tmpdf[,c("document_id","topic_id1","topic_id2"), with =F]
## On tmpdf, impute NA with most common topic
most.common.topic <- as.integer(names(sort(table(tmpdf$topic_id2),
                                           decreasing=TRUE)[1]))
tmpdf[is.na(topic_id2)][["topic_id2"]] <- most.common.topic
train.events <- merge(train.events, tmpdf, by.x = "event_document_id",
                      by.y = "document_id", all.x = TRUE)
rm(tmpdf)
## On train events, impute NA with most common topic
most.common.topic_id1 <- as.integer(names(sort(table(train.events$topic_id1),
                                               decreasing = TRUE)[1]))
most.common.topic_id2 <- as.integer(names(sort(table(train.events$topic_id2),
                                               decreasing = TRUE)[1]))
train.events[is.na(topic_id1)][["topic_id1"]] <-  most.common.topic_id1
train.events[is.na(topic_id2)][["topic_id2"]] <-  most.common.topic_id2

## -----------------------------------------------------------------------------
## SAVE
## -----------------------------------------------------------------------------
## uuid to integer to save memory
train.events$uuid <- as.integer(as.factor(train.events$uuid))
write_feather(train.events, 'train_events.f')
