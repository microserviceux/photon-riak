(ns photon.db.riak
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clj-time.core :as time]
            [photon.db :as db]
            [clojure.tools.logging :as log]
            [clj-time.format :as time-format]))

(import (io Riak))

(def photon "photon")
(defn bucket [conf]
  (:riak.default_bucket conf))

(defn nodes [conf]
  (map (fn [k] (get conf k))
       (filter #(.startsWith (name %) "riak.node")
               (keys conf))))

(defn entry-point [conf]
  (first (nodes conf)))

(defn bucket-url [rdb]
  (str "http://" (entry-point (:conf rdb)) ":8098/types/"
       photon "/buckets/" (:bucket rdb) "/keys"))
(defn riak-url [rdb id]
  (str (bucket-url (:conf rdb)) "/" id))

(defn m-riak [conf] (Riak. (bucket conf) "photon" (into-array String nodes)))
(def riak (memoize m-riak))

(defrecord RiakDB [conf]
  db/DB
  (driver-name [this] "riak")
  (fetch [this stream-name id]
    (:body (client/get (riak-url this id))))
  (delete! [this id]
    (log/info "Deleting" id)
    (client/delete (riak-url this id)))
  (delete-all! [this]
    (let [body (:body (client/get (str (bucket-url this) "?keys=true")))
          js (json/parse-string body true)
          k (first (:keys js))]
      (dorun (map #(try (db/delete! this %)
                        (catch Exception e
                          (log/debug (.getMessage e)))) (:keys js)))))
  (put [this data]
    (let [id (db/uuid)
          wrapper (json/generate-string
                   {:id_s id
                    :created_dt (db/datetime)
                    :data_s (json/generate-string data)})]
      (print (str "PUT "  (riak-url this id) "\n"))
      (print "BODY: " wrapper "\n")
      (client/put (riak-url this id) {:body wrapper :content-type :json})))
  (search [this id] (:body (client/get (riak-url this id))))
  (store [this payload]
    (.persist (riak) "__all__" "event" (json/generate-string payload)))
  (distinct-values [this k] ["events"])
  (lazy-events [this stream-name date]
    (db/lazy-events-page this stream-name date 1)) 
  (lazy-events-page [this stream-name date page]
    (let [l-date (if (string? date) (read-string date) date)
          res (map #(clojure.walk/keywordize-keys (into {} %))
                   (into [] (.eventsSince (riak) l-date stream-name page)))]
      (if (< (.size res) 1)
        []
        (concat res
                (lazy-seq (db/lazy-events-page this stream-name l-date (inc page))))))))

