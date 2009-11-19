(ns 
 #^{:doc 
"wraps ssh implimentation from jsch 
http://www.jcraft.com/jsch/examples/

exec:
http://www.jcraft.com/jsch/examples/Exec.java
http://code.google.com/p/securechannelfacade/
http://seancode.blogspot.com/2008/02/jsch-scp-file-in-java.html

shell:
http://www.jcraft.com/jsch/examples/Shell.java
http://blog.james-carr.org/2006/07/11/ssh-over-java/
http://www.mailinglistarchive.com/jsch-users@lists.sourceforge.net/msg00069.html
http://www.mailinglistarchive.com/jsch-users@lists.sourceforge.net/msg00062.html

sftp:
http://www.jcraft.com/jsch/examples/ScpTo.java
http://show.docjava.com/book/cgij/jdoc/net/ssh/jsch/ChannelSftp.html


;;
note for shell out that wildcards and such are not expanded by the shell-out/sh because it jstu uses runtime-exec.  In order to runn a shell to evaluate args before runing the programs, use sh as follows: http://www.coderanch.com/t/423573/Java-General/java/Passing-wilcard-Runtime-exec-command#1870031
"}
crane.ssh2
  (:use clojure.contrib.duck-streams)
  (:use clojure.contrib.shell-out)
  (:import [java.io 
	    BufferedReader 
	    InputStreamReader 
	    PipedOutputStream 
	    PipedInputStream
	    ByteArrayInputStream])
  (:import [com.jcraft.jsch 
	    JSch
	    Session
	    Channel
	    ChannelShell
	    ChannelExec
	    ChannelSftp]))

(defmacro with-connection [bindings & body]
  `(let ~bindings 
     (try (do 
	    (let [conn# ~(bindings 0)] 
	      (if (not (.isConnected conn#)) (.connect conn#)))
	    ~@body)
	  (finally (.disconnect ~(bindings 0))))))

(defn block-until-connected [session]
 (do
   (while (not (.isConnected session))
   (try (.connect session)
	(catch com.jcraft.jsch.JSchException e 
	  (println  "Waiting for ssh to come up")
	  (Thread/sleep 10000)
	  (block-until-connected session))))
   (println  "ssh is up.")
   session)) 

(defn session  
  "start a new jsch session and connect." 
  [#^String private-key
   #^String username
   ;;#^String password
   #^String hostname]
  (let [jsch (doto (JSch.) (.addIdentity private-key))
	session (.getSession jsch username hostname 22)
	config (doto (java.util.Properties.) 
		 (.setProperty "StrictHostKeyChecking" "no"))]
    (doto session
      ;;(.setUserInfo password)
      (.setConfig config))))

(defn sftp-channel [session]
  (.openChannel session "sftp"))

(defn exec-channel [session]
  (.openChannel session "exec"))

(defn shell-channel [session]
  (.openChannel session "shell"))

;;TODO: repalce with duck-stresms? if we get a string shoudl we test first to see if it is a valid path or assume you want ot write the string to the remote file?
(defn to-stream [source]
  (if (string? source)
    (java.io.ByteArrayInputStream. (.getBytes source "UTF-8"))
    (java.io.FileInputStream. source)))

;;TODO: should we just change these all to have a ! at the end to mathc that remote execution api?
  
;;TODO: how to merge scp and put?
(defn scp 
  "copy a file to a remote dir and new filename." 
  ([#^Session session
   source 
   #^String destination]
  (with-connection [channel (sftp-channel session)]
    (.put channel (to-stream source) destination)))
  ([#^Session session
   [source 
   #^String destination]] (scp session (java.io.File. source) destination)))

(defn put 
  "copy a file to a remote dir." 
  [#^Session session
   source 
   #^String destination]
  (with-connection [channel (sftp-channel session)]
    (.cd channel destination)
    (.put channel (to-stream source) (.getName source))))

(defn chmod
  "chmod permissions on a remote dir." 
  [#^Session session
   #^Integer permissions
   #^String remote-path]
  (with-connection [channel (sftp-channel session)]
    (.chmod channel permissions remote-path)))

(defn ls
  "chmod permissions on a remote dir." 
  [#^Session session
   #^String remote-path]
  (with-connection [channel (sftp-channel session)]
    (.ls channel remote-path)))

(def TIMEOUT 5000)

;;TODO: worth using a uuid?
(def terminator "zDONEz")
(defn terminated [c] (str c "; echo" terminator "\n"))

(defmulti read-command-output class)

(defmethod read-command-output ChannelExec 
  [channel]
  (let [in (.getInputStream channel)
        bos (java.io.ByteArrayOutputStream.)
        end-time (+ (System/currentTimeMillis) TIMEOUT)]
  (while  (and (< (System/currentTimeMillis) end-time)
	      (.isConnected channel))
    (copy in bos))
  (.toString bos)))

;;TODO: still need to remove garbage at the end after the trailing \n
;; "read up to and excluding the terminator, and drop the terminator.

;; you will see the terminator twice, once showing the command executed at the prompt, and once after the output from the command. thus, teh output string is contained between the terminator markers.
;; "

(defmethod read-command-output ChannelShell 
[channel]
(let [in (reader (.getInputStream channel))
      builder (StringBuilder.)]
  ((fn build [marked]
     (let [l (.readLine in)]
       (doto builder
	 (.append l)
	 (.append "\n"))
       (if (.contains l terminator)
	 (if marked
	   (let [s (str builder)
		 out (.substring 
		      s (+ 
			 (.indexOf s terminator)
			 (.length (str terminator "\n"))))
		 o (.substring 
		    out 0 (+ (.indexOf out terminator)))]
	     o)
	   (build true))
	 (build marked)))) false)))

(defmulti write-command (fn [ch cmd] (class ch)))

(defmethod write-command ChannelExec  
  [channel command]
    (do 
      (.setCommand channel command)
      (.setInputStream channel nil)))

(defmethod write-command ChannelShell  
  [channel command]
  (do 
    (.setInputStream 
     channel
     (ByteArrayInputStream. 
      (.getBytes (terminated command))))))

;;TODO: this might turn into a multimethod if some cases (like exec when u dont need to the oepn the channel beforehand since it is always transient, they might want to take session rather than channel as first praram.
;;TODO: stuff to make persistent shell chennel work:
;;-can't close every time.
;;-need to hold on to the same input channel?        
;;in that case read-command-output needs to be taking an input stream rather than a channel?
(defn sh! 
  "execute a shell command on a remote process. the way the command is written to the stream is determined by whether it is an exec or shell channel, but the way the output is read is consistent." 
  [#^Channel channel
   #^String command]
(write-command channel command)
 (with-connection [chan channel]
  (read-command-output channel)))


;;TODO: replace with new gzip streaming foo?
(defn push 
"
takes a list of to->from paths, turns them into tuples, and scps to -> from.

scps in parallel via pmap (hopefully.)

call with a [seesion paths :zip] to get all the paths rolled up into a tar.gz before scp'ing.  kind of a shady api there, probably want to change and provide other options like gziping each path in parallel and scping in parallel.
"
([sess paths]
 (doall 
  (pmap #(scp sess %) 
	(apply hash-map 
	       paths))))
([sess paths zip]
 (let [to-from (apply hash-map 
		      paths)]
  (do 
    (sh "sh" "-c" (apply str "tar -czf /tmp/push.tar.gz " (map first to-from)))
    (scp sess (java.io.File. "/tmp/push.tar.gz") "/root/push.tar.gz")
    (sh! sess "tar -xzf push.tar.gz")
    (doall (map 
	    (fn [[from to]] 
	      (sh! sess 
		   (str "cp " from " " to))) to-from))
    (sh "rm" "/tmp/push.tar.gz")))))
