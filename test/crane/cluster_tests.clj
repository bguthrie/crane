(ns crane.cluster-tests
  (:use crane.cluster)
  (:use crane.config)
  (:use clojure.contrib.test-is))

(deftest replace-all-strings
  (is (= "foo baz"
	 (replace-all "foo bar" [["bar" "baz"]])))
  (is (= "foo baz bag"
	 (replace-all "foo bar biz" 
		      [["bar" "baz"] ["biz" "bag"]]))))