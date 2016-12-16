#!/usr/bin/env Rscript

library(ggplot2)
library(sqldf)
library(dplyr)

con = dbConnect(SQLite(), dbname="results.db")

d = dbGetQuery( con,'select * from r')

d = d[ is.na(d$process), ]


# here prefix is TYPE-FILESYSTEM
d$fs = as.factor( unlist(strsplit(d$prefix, "-"))[1:nrow(d)*3 - 2] )
d$storage = as.factor( unlist(strsplit(d$prefix, "-"))[1:nrow(d)*3 - 1] )
d$processors = as.numeric( unlist(strsplit(d$prefix, "-"))[1:nrow(d)*3] )

d$lim_free_mem = as.factor(d$lim_free_mem)

r = d %>% filter(phase == "b", lim_free_mem == 1e+05 ) %>% group_by(fs, storage, processors) %>% summarise(obj_per_s = mean(obj_per_s))

ggplot(r, aes(processors, obj_per_s, col=storage)) + geom_point() + facet_grid(fs ~ ., switch = 'y') + scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
ggsave("tp-proc-unlimitedMemory.png")

ggplot(r, aes(processors, obj_per_s, col=fs)) + geom_point() + facet_grid(storage ~ ., switch = 'y')+ scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
ggsave("tp-proc-by-storage-unlimitedMemory.png")

myfunc = function(x){
if(x == "c")  {
	return ("cleanup") 
}else if(x == "b") {
	return ("bench")
}
	return ("precreate")
}


# plot the performance for the different storage

for (dset in c(10, 50)){

	r = filter(d, phase == "b", lim_free_mem == 1000, processors == 1, data_sets == dset )

	ggplot(r, aes(iteration, obj_per_s, col=storage)) + geom_point() + facet_grid(fs ~ ., switch = 'y') +  scale_y_log10() + ylab("objects/s") # scales="free_y",
	ggsave("tp-fs.png")

	r = d %>% filter(phase == "b", lim_free_mem == 1000, data_sets == dset) %>% group_by(fs, storage, processors) %>% summarise(obj_per_s = mean(obj_per_s))

	ggplot(r, aes(processors, obj_per_s, col=storage)) + geom_point() + facet_grid(fs ~ ., switch = 'y') +  scale_y_log10() + scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
	ggsave(paste(dset, "tp-proc-1000Memory.png", sep=""))

	ggplot(r, aes(processors, obj_per_s, col=fs)) + geom_point() + facet_grid(storage ~ ., switch = 'y') +  scale_y_log10() + scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
	ggsave(paste(dset, "tp-proc-by-storage-1000Memory.png", sep=""))


	r = d %>% filter(lim_free_mem == 1000, data_sets == dset) %>% group_by(phase, fs, storage, processors) %>% summarise(obj_per_s = mean(obj_per_s))
	r$phase = sapply(r$phase, myfunc)

	ggplot(r, aes(processors, obj_per_s, col=storage)) + geom_point() + facet_grid(fs + phase ~ ., switch = 'y') +  scale_y_log10() + scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
	ggsave(paste(dset, "tp-phases-1000Memory.png", sep=""))

	ggplot(r, aes(processors, obj_per_s, col=fs)) + geom_point() + facet_grid(storage + phase ~ ., switch = 'y') +  scale_y_log10() + scale_x_continuous(breaks=c(1,3,5,7,8,10,12)) + ylab("objects/s") # scales="free_y",
	ggsave(paste(dset, "tp-phases-by-storage-1000Memory.png", sep=""))
}


