(ns gen-ci
  (:require
   [babashka.tasks :as tasks]
   [clj-yaml.core :as yaml]
   [clojure.string :as str]
   [flatland.ordered.map :refer [ordered-map]]))

(def graalvm-version "21.0.2")

(defn run
  ([cmd-name cmd]
   (run cmd-name cmd nil))
  ([cmd-name cmd no-output-timeout]
   (let [base {:run {:name    cmd-name
                     :command cmd}}]
     (if no-output-timeout
       (assoc-in base [:run :no_output_timeout] no-output-timeout)
       base))))

(defn make-graalvm-url [arch]
  (let [myarch (case arch
                 "amd64" "x64"
                 "aarch64" "aarch64")]
    (str "https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-"
         graalvm-version
         "/graalvm-community-jdk-"
         graalvm-version
         "_linux-"
         myarch
         "_bin.tar.gz")))

(defn native-image
  [arch resource-class]
  (let [cache-key (str arch "-deps-linux-{{ checksum \"deps.edn\" }}")
        graalvm-url (make-graalvm-url arch)]
    (ordered-map
     :machine
     {:image "ubuntu-2204:2023.10.1"
      :resource_class resource-class}
     :working_directory "/home/circleci/replikativ"
     :environment {:GRAALVM_VERSION graalvm-version
                   :DTHK_PLATFORM "linux"
                   :DTHK_ARCH arch
                   :PATH "/bin:/home/circleci/graalvm/bin:/home/circleci/clojure/bin:/home/circleci/bin"
                   :JAVA_HOME "/home/circleci/graalvm/bin/java"}
     :steps
     [:checkout
      {:restore_cache {:keys [cache-key]}}
      (run "Install GraalVM"
           (format "cd /home/circleci
/bin/wget -O graalvm.tar.gz %s
/bin/mkdir graalvm || true
/bin/tar -xzf graalvm.tar.gz --directory graalvm --strip-components 1
sudo update-alternatives --install /usr/bin/java java /home/circleci/graalvm/bin/java 0
sudo update-alternatives --install /usr/bin/javac javac /home/circleci/graalvm/bin/javac 0
sudo update-alternatives --set java /home/circleci/graalvm/bin/java
sudo update-alternatives --set javac /home/circleci/graalvm/bin/javac"
                   graalvm-url))
      (run "Install Clojure"
           "cd /home/circleci
/bin/curl -sLO https://download.clojure.org/install/linux-install-1.11.1.1165.sh
/bin/chmod +x linux-install-1.11.1.1165.sh
./linux-install-1.11.1.1165.sh --prefix /home/circleci/clojure")
      (run "Install Babashka"
           "cd /home/circleci
/bin/curl -sLO https://raw.githubusercontent.com/babashka/babashka/master/install
/bin/chmod +x install
./install --dir /home/circleci/bin")
      (run "Build native image"
           "cd /home/circleci/replikativ
bb ni-cli")
      (run "Test native image"
           "cd /home/circleci/replikativ
bb test native-image")
      (run "Release native image"
           "cd /home/circleci/replikativ
bb release native-image")
      {:persist_to_workspace
       {:root "/home/circleci/"
        :paths ["replikativ/dthk"]}}
      {:save_cache
       {:paths ["~/.m2" "~/graalvm"]
        :key cache-key}}])))

(defn make-config []
  (ordered-map
   :version 2.1
   :orbs
   {:tools "replikativ/clj-tools@0"}
   :commands
   {:setup-docker-buildx
    {:steps
     [{:run
       {:name "Create multi-platform capable buildx builder"
        :command
        "docker run --privileged --rm tonistiigi/binfmt --install all\ndocker buildx create --name ci-builder --use"}}]}}
   :jobs (ordered-map
          :linux-amd64 (native-image "amd64" "large")
          :linux-aarch64 (native-image "aarch64" "arm.large"))
   :workflows (ordered-map
               :version 2
               :native-images
               {:jobs ["linux-amd64"
                       "linux-aarch64"]})))

(def skip-config
  {:skip-if-only [#"^doc\/.*"
                  #"^bb\/.*"
                  #"^dev\/.*"
                  #"^examples\/.*"
                  #"^test\/.*"
                  #".*.md$"]})

(defn get-changes
  []
  (-> (tasks/shell {:out :string} "git diff --name-only HEAD~1")
      (:out)
      (str/split-lines)))

(defn irrelevant-change?
  [change regexes]
  (some? (some #(re-matches % change) regexes)))

(defn anything-relevant?
  [change-set regexes]
  (some? (some #(not (irrelevant-change? % regexes)) change-set)))

(defn main
  []
  (let [{:keys [skip-if-only]} skip-config
        changed-files          (get-changes)]
    (when (anything-relevant? changed-files skip-if-only)
      (-> (make-config)
          (yaml/generate-string :dumper-options {:flow-style :block})
          println))))

(when (= *file* (System/getProperty "babashka.file"))
  (main))

(comment
  (def changed-files (get-changes))
  (anything-relevant? changed-files (:skip-if-only skip-config)))
