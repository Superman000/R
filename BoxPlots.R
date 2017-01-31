#Boxplot code example
boxplot(Age~IsActive,data=master, main="Age - Active vs. Dormant Targeted",
        xlab="", ylab="Age")
		
#If no freq variable is present, do the following
master$Placeholder <- 1
boxplot(Age~Placeholder,data=master, main="Age - Active vs. Dormant Targeted",
        xlab="", ylab="Age")

#Note: Set the outline=F option to exclude outliers from the plot
#ylim=c(start,end) may also be used to scale plots vertically
