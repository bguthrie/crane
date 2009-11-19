(ns crane.cluster
 (:use clojure.contrib.seq-utils)
 (:use clojure.contrib.shell-out)
 (:use clojure.contrib.duck-streams)
 (:use clojure.contrib.java-utils)
 (:use crane.ssh2)
 (:use crane.ec2)
 (:use crane.config)
 (:import java.io.File)
 (:import java.util.ArrayList))

(defn cluster-master [cluster-name] 
  (str cluster-name "-master")) 

(defn find-master
"find master finds the master for a given cluster.

if the cluster is named foo, then the master is named foo-master.

be advised that find master returns nil if the master has been reserved but is not in running state yet.
"
 [ec2 cluster]
 (first 
  (running-instances ec2 (cluster-master cluster))))

(defn master-already-running? [ec2 cluster]
 (if (find-master ec2 cluster)
      true
      false))

(defn cluster-running? [ec2 cluster n]
  (and (master-already-running? ec2 cluster)
       (already-running? ec2 cluster n)))

(defn cluster-instance-ids [ec2 cluster]
  (instance-ids 
   (concat
    (find-reservations ec2 cluster)
    (find-reservations ec2 (cluster-master cluster)))))

(defn stop-cluster 
  "terminates the master and all slaves."
  [ec2 cluster]
  (terminate-instances ec2 (cluster-instance-ids ec2 cluster)))

(defn job-tracker-url [{host :public}]
(str "http://" host ":50030"))

(defn user-bytes 
([conf host]
(let [cred (creds (:creds conf))]
  (bytes (user-data
	  (init-remote 
	   (:init-remote conf)) 
	  cred
	  host))))

([conf]
(let [cred (creds (:creds conf))]
  (bytes (user-data
	  (init-remote 
	   (:init-remote conf)) 
	  cred)))))

(defn launch-hadoop-master 
  [ec2 conf]
  (let [cluster (:group conf)
	cred (creds (:creds conf))
	master-conf (merge conf cred
		     {:group (cluster-master cluster)
		      :user-data (user-bytes conf)})
	  master (ec2-instance ec2 master-conf)
	  session (block-until-connected 
		   (session 
		    (:private-key-path master-conf) "root" 
		    (:public (attributes master))))]
      (do 
	(push session (:push conf))
	(scp session (File. (:private-key-path master-conf)) "/root/.ssh/id_rsa")
	;;TODO: the umask on the ami's /root/.ssh needs to be changed becasue it overrides our chmod 600
	(chmod session 600 "/root/.ssh/id_rsa"))
      master))

(defn launch-hadoop-slaves 
  "launch n hadoop slaves and attach to cluster master.

 TODO: you can use the same function to add slaves to a running job, but it won't take care of slaves file and such?
"
  [ec2 conf master]
  (let [cred (creds (:creds conf))]
    (ec2-instances 
     ec2 
     (merge 
      conf
      {:user-data (user-bytes conf (:private (attributes master)))   
       :monitoring false}))))

(defn launch-hadoop-cluster 
 "Launch an EC2 cluster of Hadoop instances.

 workflow for adding slaves to a cluster:
 -launch slaves with reservation
 -edit user data to have master
 -add slaves to slaves file on master
 -start services
"
([conf] (launch-hadoop-cluster (ec2 (creds (:creds conf))) conf))
([ec2 conf] 
(let [master (launch-hadoop-master ec2 conf)
      slaves (launch-hadoop-slaves ec2 conf master)
      slave-ips (map 
		 #(:private (attributes %)) 
		 slaves)
      slaves-str (apply 
		  str 
		   (interleave slave-ips (repeat "\n")))
      session (block-until-connected 
		   (session 
		    (:private-key-path (creds (:creds conf))) "root" 
		    (:public (attributes master))))]
      (do 
	(scp 
	 session 
	 slaves-str
	 "/usr/local/hadoop-0.20.1/conf/slaves"))
      session)))

(defn tracker
"gets the url for job tracker."
 [ec cluster]
  (job-tracker-url 
   (attributes 
    (find-master ec cluster))))