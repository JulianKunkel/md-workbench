#!/usr/bin/env Rscript

library(ggplot2)

options(echo=FALSE)
args = commandArgs(trailingOnly = TRUE)
input = args[1]

# now we have to read three files
d1 = read.csv(sprintf("%s-create-0.csv", input))

d1$type = as.factor("create")
print(sprintf("create quantiles 0.1: %e 0.9: %e", quantile(d1$runtime, 0.1), quantile(d1$runtime,0.9)))
print(summary(d1$runtime))

d2 = read.csv(sprintf("%s-read-0.csv", input))
d2$type = as.factor("read")
print(sprintf("read quantiles 0.1: %e 0.9: %e", quantile(d2$runtime, 0.1), quantile(d2$runtime,0.9)))
print(summary(d2$runtime))

d3 = read.csv(sprintf("%s-delete-0.csv", input))
d3$type = as.factor("delete")
print(sprintf("delete quantiles 0.1: %e 0.9: %e", quantile(d3$runtime, 0.1), quantile(d3$runtime,0.9)))
print(summary(d3$runtime))

d4 = read.csv(sprintf("%s-stat-0.csv", input))
d4$type = as.factor("stat")
print(sprintf("stat quantiles 0.1: %e 0.9: %e", quantile(d4$runtime, 0.1), quantile(d4$runtime,0.9)))
print(summary(d4$runtime))



d = rbind(d1, d2, d3, d4)

p = ggplot(d, aes(x=time,y=runtime,col=type)) + geom_point(alpha=.4, size=1) + xlab("time") + ylab("runtime") + scale_y_log10() +   theme(legend.position="bottom")
ggsave(sprintf("%s-timeline.pdf", input), plot=p)

p = ggplot(d, aes(x=runtime,col=type)) + geom_density(alpha=.2)  + scale_x_log10() +   theme(legend.position="bottom") # +  geom_histogram(aes(y=..density..), binwidth=.5, colour="black", fill="white")
ggsave(sprintf("%s-density.pdf", input), plot=p)

p = ggplot(d, aes(x=type, y=runtime, fill=type)) + xlab("") + geom_boxplot() + scale_y_log10() +   theme(legend.position="bottom")
ggsave(sprintf("%s-runtime.pdf", input), plot=p)

d = d[ order(d$runtime),]
d$pos = (1:nrow(d))/nrow(d)
p = ggplot(d, aes(x=pos, y=runtime,col=type)) + geom_point(alpha=.4, size=1) + xlab("fraction of measurements") + ylab("runtime") + scale_y_log10() +   theme(legend.position="bottom")
ggsave(sprintf("%s-waittimes-all.pdf", input), plot=p)

p = ggplot(d, aes(x=pos, y=runtime,col=type)) + geom_point(alpha=.4, size=1) + xlab("fraction of measurements") + ylab("runtime") + scale_y_log10() + facet_grid(type ~ .) + theme(legend.position="none")
ggsave(sprintf("%s-waittimes.pdf", input), plot=p)


d = d[ order(d$type, d$runtime),]
d$pos = 1
d$pos[1:nrow(d1)] = (1:nrow(d1))/nrow(d1)
d$pos[(1*nrow(d1)+1):(2*nrow(d1))] = (1:nrow(d1))/nrow(d1)
d$pos[(2*nrow(d1)+1):(3*nrow(d1))] = (1:nrow(d1))/nrow(d1)
d$pos[(3*nrow(d1)+1):(4*nrow(d1))] = (1:nrow(d1))/nrow(d1)
p = ggplot(d, aes(x=pos, y=runtime,col=type)) + geom_point(alpha=.4, size=1) + xlab("fraction of measurements") + ylab("runtime") + scale_y_log10() + facet_grid(type ~ .) + theme(legend.position="none")
ggsave(sprintf("%s-waittimes-ind.pdf", input), plot=p)
