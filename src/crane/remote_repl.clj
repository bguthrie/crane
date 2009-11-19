(ns 
 #^{:doc 
"
-snapshot jars, gzip, and push it to remote machine.
-start repl on remote machine via ssh with jars on the classpath.
-remote-eval code and get results back from remote repl.

http://en.wikibooks.org/wiki/Clojure_Programming/Examples/REPL_Socket
http://richhickey.github.com/clojure-contrib/server-socket-api.html
http://travis-whitton.blogspot.com/2009/09/hot-code-swapping-with-clojure.html
http://clojure101.blogspot.com/2009/05/creating-clojure-repl-in-your.html


"}
 crane.remote-repl
 (:import [java.net ServerSocket Socket SocketException]
	  [java.io InputStreamReader OutputStreamWriter]
	  [clojure.lang LineNumberingPushbackReader])
 (:use [clojure.contrib shell-out server-socket duck-streams])
 (:use crane.ec2)
 (:use crane.ssh2))


(defn research-repl 
"
build the java jvm startup string for the remote clojure process used to launch remote repls on sockets.

inlcludets paths for the classpath and memory arg.

useages: (def my-repl (research-repl \"/some/path/*:/some/jars/a.jar\"))
"
  ([path] (research-repl path "1024m"))  
  ([path memory] 
  (str 
  "java -Xmx" memory
  " -cp " path  
  " jline.ConsoleRunner clojure.lang.Repl")))

(defn safe-read [s]
 (try (read-string s)
      (catch java.lang.RuntimeException _ s)))

;;TODO: should all reading is the ByteArrayOutputStream approach in remote_repl read-command-output?
(defn eval!
"
eval some code in the remote repl session on the given socket.

setup server and client as
push necessary deps and star repl on remote machine.
start a server with: (def server (create-repl-server 8080))
get a client socket with: (def client (new Socket \"localhost\" 8080))


a perhaps better implimentation would be:

 (defn eval! [socket expr]
  (let [rdr (LineNumberingPushbackReader. 
	     (reader (.getInputStream socket)))
	wtr (writer (.getOutputStream socket))]
    (binding [*out* wtr]
      (prn expr)
      (flush)
      (read rdr) ;;reads and discards the clojure.core=>
      (read rdr))))

...but the read function blows up when the stream contains java objects that have been printed #< > or exceptions.
"
  [socket expr]
  (let [in (reader (.getInputStream socket))
	wtr (writer (.getOutputStream socket))]
    (binding [*out* wtr]
      (prn expr)
      (flush)
      (let [l (.readLine in)]
	;;deals with the case of the repl form: "clojure.core=> 6"
	(if-let [result (second (.split l ">"))]
	  (safe-read result)
	  l)))))