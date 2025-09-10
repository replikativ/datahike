(ns gen-ci
  (:require
   [babashka.tasks :as tasks]
   [clj-yaml.core :as yaml]
   [clojure.string :as str]
   [flatland.ordered.map :refer [ordered-map]]))

(def graalvm-version "24.0.2")

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

(defn build-native-image
  [arch resource-class]
  (let [cache-key (str arch "-deps-linux-{{ checksum \"deps.edn\" }}")
        graalvm-url (make-graalvm-url arch)]
    (ordered-map
     :machine
     {:image "ubuntu-2204:2023.10.1"
      :resource_class resource-class}
     :working_directory "/home/circleci/replikativ"
     :environment {:GRAALVM_VERSION graalvm-version
                   :GRAALVM_HOME "/home/circleci/graalvm"
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
      (run "Build libdatahike"
           "cd /home/circleci/replikativ
bb ni-compile")
      (run "Test native image"
           "cd /home/circleci/replikativ
bb test native-image")
      (run "Test babashka pod"
           "cd /home/circleci/replikativ
bb test bb-pod")
      (run "Test libdatahike"
           "cd /home/circleci/replikativ
bb test libdatahike")
      {:persist_to_workspace
       {:root "/home/circleci/"
        :paths ["replikativ/dthk" "replikativ/libdatahike/target"]}}
      {:save_cache
       {:paths ["~/.m2" "~/graalvm"]
        :key cache-key}}])))

(defn release-artifacts
  [arch]
  (let [cache-key (str arch "-deps-linux-{{ checksum \"deps.edn\" }}")]
    (ordered-map
     :executor "tools/clojurecli"
     :working_directory "/home/circleci/replikativ"
     :environment {:DTHK_PLATFORM "linux"
                   :DTHK_ARCH arch}
     :steps
     [:checkout
      {:restore_cache {:keys [cache-key]}}
      {:attach_workspace {:at "/home/circleci"}}
      (run "Release native image"
           "cd /home/circleci/replikativ
bb release native-image")
      (run "Release libdatahike"
           "cd /home/circleci/replikativ
bb release libdatahike")
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
          :build-linux-amd64 (build-native-image "amd64" "large")
          :build-linux-aarch64 (build-native-image "aarch64" "arm.large")
          :release-linux-amd64 (release-artifacts "amd64")
          :release-linux-aarch64 (release-artifacts "aarch64"))
   :workflows (ordered-map
               :version 2
               :native-images
               {:jobs ["build-linux-amd64"
                       "build-linux-aarch64"
                       {"release-linux-amd64"
                        {:context ["dockerhub-deploy"
                                   "github-token"]
                         :filters {:branches {:only "main"}}
                         :requires ["build-linux-amd64"]}}
                       {"release-linux-aarch64"
                        {:context ["dockerhub-deploy"
                                   "github-token"]
                         :filters {:branches {:only "main"}}
                         :requires ["build-linux-aarch64"]}}]})))

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
  (anything-relevant? changed-files (:skip-if-only skip-config))
  (-> (make-config) 
      (yaml/generate-string :dumper-options {:flow-style :block})
      println))
