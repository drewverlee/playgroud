(ns play
  (:require [clojure.set :as set]
            [ubergraph.core :as uber]
            [loom.alg :as alg]
            [hugsql.core :as hugsql]
            [table-spec.core :as t]
            [clojure.spec.gen.alpha :as gen]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [honeysql-postgres.format :refer :all]
            [honeysql-postgres.helpers :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.spec.alpha :as s]))


(def db {:dbtype "postgresql"
         :dbname "db"
         :host "127.0.0.1"
         :port "5439"
         :user "postgres"
         :sslfactory "org.postgresql.ssl.NonValidatingFactory"})

(hugsql/def-db-fns "db.sql")


(-> (create-table :films)
    (with-columns [[:code (sql/call :char 5) (sql/call :constraint :firstkey) (sql/call :primary-key)]
                   [:title (sql/call :varchar 40) (sql/call :not nil)]
                   [:did :integer (sql/call :not nil)]
                   [:date_prod :date]
                   [:kind (sql/call :varchar 10)]])
    (j))



(-> {:connection-uri "jdbc:postgresql:postgres" :schema "public"}
    (t/tables)
    (t/register))

(defn db->graph-uber
  [{:keys [fk_table fk_column pk_table pk_column]}]
  [(keyword fk_table) (keyword pk_table) {:fk_column fk_column :pk_column pk_column}])

(->> (get-fk-dependencies db)
     (map db->graph)
     (apply uber/digraph)
     (alg/bf-traverse))


[{:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"}
 {:fk_table "dogs", :fk_column "owner", :pk_table "persons", :pk_column "id"}]

(->> (uber/digraph [:dogs :persons] {:fk_column "owner" :pk_column "id"})
     (alg/bf-traverse))


;; (db->graph {:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"})

;; [:fk_table :pk_table] {:fk_column :pk_column}
;; [:persons :persons] {:fk_column "father" :pk_column "id"}

(def g (uber/graph [:a :b {:fk_column :id :pk_column :pid}]))

(defn f [n m d] {:n n :m m :edge (map #(uber/attrs g %) m) :d d})

(alg/bf-traverse (uber/graph [:a :b {:fk_column :id :pk_column :pid}]) :a :f f)

;; => ({:n :a, :m {:a nil}, :d 0} {:n :b, :m {:a nil, :b :a}, :d 1})


;; produce a insert statment and the data to insert with it.
;; you can get attrs from uber/graphs
(uber/attrs g [:a :b] [:fk_column :pk_column]) 
(map #(uber/attrs g [% %1]) {:a :b})
(map #(uber/attrs g %){:a :b})

;; ;; NOTE create structure using honeysql
;; http://www.chesnok.com/daily/2013/11/19/everyday-postgres-insert-with-select/comment-page-1/
;; On edge to next node get reference and update structure




;; ["INSERT INTO dogs (id) VALUES ((SELECT id FROM persons LIMIT ?))" 1]

(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))


;;(create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})

;;=> {:insert-into :persons, :values [{:father {:select [:id], :from [:persons], :limit 1}}]}
;; working
(defn dfs
  ([n g] (dfs [n] #{} g))
  ([nxs v g]
   (let [n (peek nxs)
         v (conj v n)]
     (when n
       (cons n (dfs (filterv #(not (v %)) (concat (pop nxs) (n g))) v g))))))

(dfs :a {:a #{:b} :b #{:b}})

(def tables
  [{:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "owner", :pk_table "persons", :pk_column "id"}])


(def tables-2
  [{:fk_table "persons", :fk_column "father", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "owner", :pk_table "persons", :pk_column "id"}
   {:fk_table "dogs", :fk_column "home", :pk_table "address", :pk_column "id"}])

(reduce
 (fn [coll {:keys [fk_table fk_column pk_table pk_column]}]
   (if (contains? coll fk_table)
     (update-in coll [fk_table] assoc pk_table {:fk_column fk_column :pk_column pk_column})
     (assoc coll fk_table {pk_table {:fk_column fk_column :pk_column pk_column}})))
 {} tables-2)



(def db
        {"persons" {"persons" {:fk_column "father", :pk_column "id"}},
         "dogs"    {"persons" {:fk_column "owner", :pk_column "id"},
                    "address" {:fk_column "home", :pk_column "id"}}
         "address" {}})

(defn table->db
  [table]
  (reduce
    (fn [coll {:keys [fk_table fk_column pk_table pk_column]}]
      (if (contains? coll fk_table)
        (update-in coll [fk_table] assoc pk_table {:fk_column fk_column :pk_column pk_column})
        (assoc coll fk_table {pk_table {:fk_column fk_column :pk_column pk_column}})))
    {} table))

(defn db->graph
  [db]
  (reduce-kv
   (fn [m k v]
     (assoc m k (into #{} (keys v))))
   {} db))

(defn keyify
  [coll]
  (map #(reduce-kv (fn [m k v] (assoc m k (keyword v))) {} %) coll))










;;(create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})
(create-insert {:father 1 :name "joe"} {:fk_table :persons, :fk_column :father, :pk_table :persons, :pk_column :id})

(s/def ::id int?)
(s/def ::father int?)
(s/def ::name string?)
(s/def ::owner ::id)

(s/def ::persons (s/keys :req-un [::id ::father ::name]))
(s/def ::dogs (s/keys :req-un [::id ::owner ::name]))


(defn g [t] (first (gen/sample (s/gen (qualifier t)) 1)))

(g ::dogs)

(defn qualifier [n] (keyword (-> *ns* ns-name str) (name n)))

;;=> {:insert-into :persons, :values [{:father {:select [:id], :from [:persons], :limit 1}}]}


(defn ->inserts
  [tables]
  (let [db (->> tables keyify table->db)]
    (->> db
        db->graph
        (dfs :dogs)
        (reduce (fn [c t]
                  (conj c
                        (map (fn [r] (create-insert (g t) r))
                             (t (group-by :fk_table (keyify tables))))))
                []))))


(->> tables
     ->inserts
     flatten
     (map sql/format)
     first)



"INSERT INTO dogs (id, owner, name)
           VALUES (?, (SELECT id FROM persons LIMIT ?), ?) NULL"

 ;; (({:insert-into :dogs, :values [({:owner {:select [:id], :from [:persons], :limit 1}} {:id 0, :owner -1, :name ""})]}) ({:insert-into :persons, :values [({:father {:select [:id], :from [:persons], :limit 1}} {:id -1, :father -1, :name ""})]})))

;; Example of insert with select
;; INSERT into foo_bar (foo_id, bar_id)
;; VALUES ((select id from foo where name = 'selena'),
;;          (select id from bar where type = 'name'));    

;;Example of how to insert with select
(-> (insert-into :dogs)
    (values [{:id {:select [:id] :from [:persons] :limit 1}}])
    sql/format)



(defn create-insert
  [m {:keys [fk_table fk_column pk_table pk_column]}]
  (-> (insert-into fk_table)
      (values [(merge m {fk_column {:select [pk_column] :from [pk_table] :limit 1}})])))
