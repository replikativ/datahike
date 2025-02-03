(ns gen-ci
  (:require
   [babashka.tasks :as tasks]
   [clj-yaml.core :as yaml]
   [clojure.string :as str]
   [flatland.ordered.map :refer [ordered-map]]))

(def graalvm-version "23.0.2")

(defn run
  ([cmd-name cmd]
   (run cmd-name cmd nil))
  ([cmd-name cmd no-output-timeout]
   (let [base {:run {:name    cmd-name
                     :command cmd}}]
     (if no-output-timeout
       (assoc-in base [:run :no_output_timeout] no-output-timeout)
       base))))

(defn make-graalvm-url [arch platform]
  (let [myarch (case arch
                 "amd64" "x64"
                 "aarch64" "aarch64")]
    (str "https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-"
         graalvm-version
         "/graalvm-community-jdk-"
         graalvm-version
         "_"
         platform
         "-"
         myarch
         "_bin.tar.gz")))

(defn build-native-image-linux
  [arch image resource-class]
  (let [cache-key (str arch "-deps-linux-{{ checksum \"deps.edn\" }}")
        graalvm-url (make-graalvm-url arch "linux")]
    (ordered-map
     :machine
     {:image image
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
      {:persist_to_workspace
       {:root "/home/circleci/"
        :paths ["replikativ/dthk"]}}
      {:save_cache
       {:paths ["~/.m2" "~/graalvm"]
        :key cache-key}}])))

(defn build-native-image-macos
  [arch]
  (let [cache-key (str arch "-deps-macos-{{ checksum \"deps.edn\" }}")
        graalvm-url (make-graalvm-url arch "macos")]
    (ordered-map
     :macos
     {:xcode "13.4.1"}
     :resource_class "macos.m1.medium.gen1"
     :environment {:GRAALVM_VERSION graalvm-version
                   :DTHK_PLATFORM "macos"
                   :DTHK_ARCH arch
                   :GRAALVM_HOME "/Users/distiller/graalvm/Contents/Home"
                   :JAVA_HOME "/Users/distiller/graalvm/Contents/Home"}
     :steps
     [:checkout
      {:restore_cache {:keys [cache-key]}}
      (run "Install Rosetta"
           "sudo /usr/sbin/softwareupdate --install-rosetta --agree-to-license")
      (run "Install GraalVM"
           (format "wget -O graalvm.tar.gz %s
mkdir graalvm || true
tar -xzf graalvm.tar.gz --directory graalvm --strip-components 1
ls -la graalvm
pwd"
                   graalvm-url))
      (run "Install Clojure"
           ".circleci/scripts/install-clojure /usr/local")
      (run "Install Babashka"
           "bash <(curl -s https://raw.githubusercontent.com/borkdude/babashka/master/install) --dir $(pwd)
            ls -lahrt
            echo $PATH
            sudo mv bb /usr/local/bin
            bb --version
            /usr/local/bin/bb --version")
      (run "Build native image"
           "ls -lahrt
bb ni-cli")
      (run "Test native image"
           "bb test native-image")
      {:persist_to_workspace
       {:root "/home/circleci/"
        :paths ["replikativ/dthk"]}}
      {:save_cache
       {:paths ["~/.m2" "~/graalvm"]
        :key cache-key}}])))

(defn release-native-image
  [platform arch]
  (let [cache-key (str arch "-deps-linux-{{ checksum \"deps.edn\" }}")]
    (ordered-map
     :executor "tools/clojurecli"
     :working_directory "/home/circleci/replikativ"
     :environment {:DTHK_PLATFORM platform
                   :DTHK_ARCH arch}
     :steps
     [:checkout
      {:restore_cache {:keys [cache-key]}}
      {:attach_workspace {:at "/home/circleci"}}
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
          :build-linux-amd64 (build-native-image-linux "amd64" "ubuntu-2204:2024.11.1" "large")
          :build-linux-aarch64 (build-native-image-linux "aarch64""ubuntu-2204:2024.11.1" "arm.large")
          :build-macos-amd64 (build-native-image-macos "amd64")
          :release-linux-amd64 (release-native-image "linux" "amd64")
          :release-linux-aarch64 (release-native-image "linux" "aarch64"))
   :workflows (ordered-map
               :version 2
               :native-images
               {:jobs ["build-linux-amd64"
                       "build-linux-aarch64"
                       "build-macos-amd64"
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
      println)

  (make-graalvm-url "amd64" "macos"))
