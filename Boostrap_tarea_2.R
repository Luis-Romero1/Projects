######Tarea_2###########
#Tenemos una base de datos Futbol 
#contiene n=8698, GL es el número de goles que anot+o el equipo local
#GV corresponde a los goles anotados por el equipo visitante
#X= Goles anotados en el partido
#Z la diferencia absoluta de goles entre los dos equipos 
getwd()
setwd("C:/Users/Luis_Romero/Documents/GitHub/BigData20202/Tarea2")
getwd()
data <- read.csv("Futbol.csv")
data[8699,]

attach(data)
dim(data)
#######################################
#Preguntas
#######################################

#1asumiendo que x es poiss(l) UTILIZANDO Bootstrap , encuentre intervalo del 85%
# para P(X>5).
GT <- GL+GV
GT <- GT[!is.na(GT)]
n <- length(GT)
lambda <- mean(GT)
nsim <- 50000
nsim2 <- 200000
muestra = rpois(n,lambda)
pro.est.boot <-c()
t <- proc.time()
for (i in 1:nsim2){
  muestra = rpois(n,lambda)
  pro.est.boot[i] = 1-ppois(5,lambda=mean(muestra))
  
}

hist(pro.est.boot,breaks = 1000,freq = FALSE)
qua0ntile(pro.est.boot,c(0.075,0.925))
proc.time() - t

library(doParallel)
parallel::detectCores()## detecta cuantos procesadores tienes para trabajar , podrias dejar alguno si estas viendo 
###por ejemplo netflix 
##aqui no se utilizan los for tradicionales , se utilizan los for each 
cl <- makeCluster(7)
###luego de escoger el número de procesadores los tenemos que preparar para el proceso
registerDoParallel(cl)
t <- proc.time()

###un parametro importante es .combine=rbind ya que este se encarga de 
###combinar los resutados de cada procesador en un solo vector, pero si no se pone es que 
###lo tirara en una lista , rbind los combina en un vector  y %dopar% es un comandito 
###para hacer todo en paralelo 
result <- foreach(i=1:nsim2, .combine=rbind) %dopar% {
  muestra = rpois(n,lambda)
  1-ppois(5,lambda=mean(muestra))
  
  ###este ulitimo comando es lo que guardara para todas las simulaciones
}
hist(result,breaks = 1000,freq = FALSE)
quantile(result,c(0.075,0.925))
stopCluster(cl)
proc.time() - t




cl <- makeCluster(6)
registerDoParallel(cl)
t <- proc.time()
result <- foreach(i=1:nsim2, .combine=rbind) %dopar% {
muestra = rpois(n,lambda)
1-ppois(5,lambda=mean(muestra))
}
proc.time() - t
stopCluster(cl)

########################################################
#Suponga que X tiene distribución desconocida,
#Contestar lo anterior utilizando boostrap
t <- proc.time()
pro.est.boot <- c()
for (i in 1:nsim2){
  muestra = sample(GT,n,replace= TRUE)
  pro.est.boot[i] <- mean(muestra>5)
  
}
hist(pro.est.boot,breaks = 1000,freq = FALSE)
quantile(pro.est.boot,c(0.075,0.925))
proc.time() - t

cl <- makeCluster(6)
registerDoParallel(cl)
t <- proc.time()

result <- foreach(i=1:nsim2, .combine=rbind) %dopar% {
  muestra = sample(GT,n,replace= TRUE)
  mean(muestra>5)
}
hist(result,breaks = 100,freq = FALSE)
quantile(result,c(0.075,0.925))
stopCluster(cl)
proc.time() - t
#######################################################

#Suponga que Z se distribuye desconocida , encuentre un intervalo del 99% de confianza para la probabilidad
#de que un partido del futbol español tenga un empate 
dif <-abs(GL-GV)
dif <- dif[!is.na(dif)]
n <- length(dif)
lambda <- mean(dif)

t <- proc.time()
pro.est.boot <- c()
for (i in 1:nsim){
  muestra = sample(dif,n,replace= TRUE)
  pro.est.boot[i] <- mean(muestra==0)
  
}
hist(pro.est.boot,breaks = 100,freq = FALSE)
quantile(pro.est.boot,c(0.005,0.995))
proc.time() - t

cl <- makeCluster(5)
registerDoParallel(cl)
t <- proc.time()
result <- foreach(i=1:nsim2, .combine=rbind) %dopar% {
  muestra = sample(dif,n,replace= TRUE)
  mean(muestra==0)
}
hist(result,breaks = 100,freq = FALSE)
quantile(result,c(0.005,0.995))
stopCluster(cl)
proc.time() - t


############################################################
############ Sea W=GL-GV y que W se distribuye de forma desconocida
###encuentre un intervalo al 80% de confianza para que la porbabilidad 
#de que el equipo de Barcelona gane por mas de dos goles como visitante 
##Realize 50000 simulaciones 

data_4 <- data[Visitant=="F.C. Barcelona",]
#dim(data_4)
w <- data_4$GL-data_4$GV
w <- w[!is.na(w)]
n1 <- length(w)
nsim1=300000

t <- proc.time()
pro.est.boot <- c()
for (i in 1:nsim1){
  muestra = sample(w,n1,replace = TRUE)
  pro.est.boot[i] <- mean(muestra < -2)
  
}

hist(pro.est.boot,breaks = 100,freq = FALSE)
quantile(pro.est.boot,c(0.1,0.9))
proc.time() - t

t <- proc.time()
cl <- makeCluster(5)
registerDoParallel(cl)

result <- foreach(i=1:nsim1, .combine=rbind) %dopar% {
  muestra = sample(w,n1,replace = TRUE)
  mean(muestra< -2)
}
quantile(result,c(0.005,0.995))
hist(result,breaks = 100,freq = FALSE)
proc.time() - t
stopCluster(cl)

