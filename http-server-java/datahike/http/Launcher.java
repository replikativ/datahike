package datahike.http;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.RT;

/**
 * Minimal JVM launcher for the standalone server.
 *
 * Keeping this entry point in Java lets the release JAR load Clojure sources
 * on its target JVM. AOT-compiling the full dependency graph on the build JVM
 * can freeze build-runtime probes (notably GraalVM detection) into bytecode and
 * make the same JAR fail or behave differently on an ordinary OpenJDK runtime.
 */
public final class Launcher {
    private Launcher() {}

    public static void main(String[] args) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.http.main"));

        IFn main = Clojure.var("datahike.http.main", "-main");
        main.applyTo(RT.seq(args));
    }
}
