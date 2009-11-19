(ns 
#^{:doc 
"
a lib for interacting with s3.

todo: use or merge with: http://github.com/stuartsierra/altlaw-backend/blob/802ac86dff0a5d1c3980df6fbd101ed429d3c5e8/src/org/altlaw/util/s3.clj
"}
crane.s3
  (:use clojure.contrib.duck-streams)
  (:import java.io.File)
  (:import org.jets3t.service.S3Service)
  (:import org.jets3t.service.impl.rest.httpclient.RestS3Service)
  (:import org.jets3t.service.model.S3Object)
  (:import org.jets3t.service.security.AWSCredentials)
  (:import org.jets3t.service.utils.ServiceUtils))

(defn s3-connection 
  ([{access-key :key secret-key :secretkey}] 
     (s3-connection access-key secret-key))
  ([k sk] (RestS3Service. (AWSCredentials. k sk))))

(defn buckets [s3] (.listAllBuckets s3))

(defn objects
"
http://jets3t.s3.amazonaws.com/api/index.html

list the objects in a bucket:
s3 bucket -> objects

list the objects in a bucket at a key:
s3 bucket key -> objects

example: (pprint 
          (objects 
           (s3-connection flightcaster-creds) 
            \"somebucket\" \"some-folder\"))
"
  ([s3 bucket-name] 
     (.listObjects s3 (.getBucket s3 bucket-name)))
  ([s3 bucket-root rest] 
     (.listObjects s3 (.getBucket s3 bucket-root) rest nil)))

(defn folder? [o]
  (or (> (.indexOf o "$folder$") 0)
      (> (.indexOf o "logs") 0)))

(defn without-folders [c]
  (filter #(not (folder? (.getKey %))) c))

(defn create-bucket [s3 bucket-name]
  (.createBucket s3 bucket-name))

(defn mkdir [path] (.mkdir (File. path)))


;;TODO: change a lot of the multimethod foo to be like copy from duck-streams
(defn puts 
([connection bucket key data] [(class key) (class data)])
([connection bucket data] [(class data)]))

(defmulti s3-put puts)

(defmethod 
#^{:doc
"
http://jets3t.s3.amazonaws.com/api/index.html

Create an object representing a file:
bucket, file -> s3-object.
"}
s3-put [java.io.File]
[s3 bucket-name file]
  	(let [bucket (.getBucket s3 bucket-name) s3-object (S3Object. bucket file)]
      (.putObject s3 bucket s3-object)))

(defmethod 
#^{:doc
"
Create an object representing text data:
bucket, key, string -> s3-object
"}
s3-put [String String] 
  [s3 bucket-name key data]
  (let [bucket (.getBucket s3 bucket-name)  
        s3-object (S3Object. bucket key data)]
    (.putObject s3 bucket s3-object)))

(defmethod
#^{:doc
"
Create an object representing a file at a certian key:
bucket, file, key -> s3-object.
"}
s3-put [String java.io.File]
  [connection bucket-name key file]
    (let [bucket (.getBucket connection bucket-name) s3-object (S3Object. bucket file)]
	  (.setKey s3-object key)
	  (.putObject connection bucket s3-object)))

(defn obj-to-str [obj]
  (ServiceUtils/readInputStreamToString (.getDataInputStream obj) "UTF-8"))

;;returns as string
(defn s3-get [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)]
    (obj-to-str obj)))

(defn files [dir]
  (for [file (file-seq (File. dir))
	      :when (.isFile file)]
	     file))

;;TODO: multimethod like copy s3->local and local->s3 for (clj,string,file,dir).
(defn s3->clj
"read the object(s) at the root/rest s3 uri into memory.

apply the rdr passed in to read the raw strings from the files."
 [s3 root-bucket rest rdr]
  (let [files (without-folders (objects s3 root-bucket rest))
	downloaded-files (for [f files] (rdr (s3-get s3 root-bucket (.getKey f))))]
    downloaded-files))

(defn dir->s3
"create a new bucket b.

copy everything from dir d to bucket b."
[s3 d b]
    (create-bucket s3 b)
    (dorun 
      (for [f (files d)]
	     (s3-put s3 b f))))

(defn s3->dir
"create a new dir d.

copy everything from bucket b to dir rooted at d with each object at dir/bucket-name/key."
 [s3 b d]
;;TODO: this probably doesn't work -> need to mkdir for each of the paths that are part fo bucketname and key.
    (mkdir d)
    (dorun 
      (for [obj (objects b)
	    cont (s3-get s3 b obj)]
	(spit (file-str d (.getBucketName obj) (.getKey obj)) cont))))