# Kupcr
Reference implementation of a scalable, reactive SQL Query engine using : **Ku**bernetes, **P**resto, Python,
 **C**elery and **R**edis

## Description
With the explosion of data, many companies have a need for scalabe SQL analytical query engine. 
For analytical pipelines Presto serves the need well. However, it gets tedious to run and maintain 
that service on more than one cluster. This project is an attempt to provide such a service which 
is scalable, reactive and easier to maintain. Reactive in this context is intended to mean a Reactive System. Which is described as
reslient, responsive, elastic and message-driven.


## Setup

### Setup Kubernetes Cluster (GKE)

	gcloud container --project "$(PROJECT_ID)" clusters create "$(CLUSTER_NAME)" --zone "$(ZONE)" --machine-type "n1-standard-2" --num-nodes "1" --min-nodes "1" --max-nodes "4" --preemptible  --image-type "COS" --enable-autoscaling --disk-size "50" --scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" --network "default" --cluster-version=1.15 --addons HorizontalPodAutoscaling,HttpLoadBalancing --no-enable-autoupgrade --enable-autorepair
	kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=$(GCLOUD_USER)

### Setup Kubernetes Cluster with RabbitMQ and Redis.

    helm install myrabbitmq --set rabbitmq.username=guest,rabbitmq.password=guest stable/rabbitmq
    helm install myredis --set cluster.enabled=false,usePassword=false stable/redis

Setup service account :

	kubectl create serviceaccount prestosvcact --namespace default
	kubectl create clusterrolebinding presto-admin-binding --clusterrole=cluster-admin --serviceaccount=default:prestosvcact


### Helm install

    helm install mykupcr ./charts/kupcr --wait