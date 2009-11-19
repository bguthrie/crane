(ns crane.config
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.java-utils)
  (:import [com.xerox.amazonws.ec2 Jec2 
	    InstanceType LaunchConfiguration
	    ImageType EC2Exception
	    ReservationDescription
	    ReservedInstances]))

(def instance-types
 {:m1.small InstanceType/DEFAULT
  :m1.large InstanceType/LARGE           
  :m1.xlarge InstanceType/MEDIUM_HCPU
  :c1.medium InstanceType/XLARGE
  :c1.xlarge InstanceType/XLARGE_HCPU})

;;TODO: thjis fn belogs with the string-foo
(defn replace-all
"replaces all of the left hand side tuple members with right hand side tuple members in the provided string."
[s pairs]
(if (= 0 (count pairs)) s
 (let [[old new] (first pairs)]
   (replace-all (.replaceAll s old new) (rest pairs)))))

(defn user-data 
"read user data file from the config-path given and replace the aws key and secrey key with those from the creds-path given."
([config-path creds]
  (let [data (slurp* config-path)]
    (replace-all data
      [["AWS_ACCESS_KEY_ID" (:key creds)]
       ["AWS_SECRET_ACCESS_KEY" (:secretkey creds)]])))

([config-path creds master-host]
  (replace-all (user-data config-path creds)
      [["MASTER_HOST" master-host]])))

(defn bytes [x] (.getBytes x))

(defn conf
  "provide the path to your config info directory containing aws.clj
for now just ami:
 
 {:ami \"ami-xxxxxx\"}
 
useage: (read-string (slurp* (conf \"/foo/bar/creds/\"))))
 
"
  [path]
  (let [config (read-string 
		(slurp* (file-str path "aws.clj")))]
    (merge config
	   {:instance-type ((:instance-type config) instance-types)})))

(defn init-remote
"
provide the path to your hadoop config directory containing 
hadoop-ec2-init-remote.sh
"
[path] (file-str path "hadoop-ec2-init-remote.sh"))

(defn creds 
"provide the path to your creds home directory containing creds.clj
key secrey-key pair stored in map as:

 {:key \"AWSKEY\"
  :secretkey \"AWSSECRETKEY\"}
  :keyname \"aws rsa key file name\"}

useage: (read-string (slurp* (creds \"/foo/bar/creds/\"))))

"
[path] (read-string (slurp* (file-str path "creds.clj"))))