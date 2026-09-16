package datahike.impl;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import org.graalvm.nativeimage.hosted.Feature;

/**
 * Loads the Clojure namespaces on one thread before analysis starts.
 *
 * With --initialize-at-build-time, native-image initializes the AOT
 * namespace classes from its parallel analysis workers. Clojure's
 * `require` is not safe under that: `ns` marks a lib loaded before its
 * body has run, so a second thread can skip the load and `refer` vars
 * that are not yet defined. On CI that surfaced intermittently as
 * "IllegalAccessError: pprint is not public" from core.async's
 * ioc_macros while another worker was still loading clojure.pprint.
 *
 * Once everything is loaded here, the analysis workers only find
 * initialized classes.
 */
public class LoadNamespacesFeature implements Feature {

    private static final String[] NAMESPACES = {
        "datahike.impl.libdatahike",
        "datahike.api",
        "datahike.cli"
    };

    @Override
    public void beforeAnalysis(BeforeAnalysisAccess access) {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(access.getApplicationClassLoader());
        try {
            IFn require = Clojure.var("clojure.core", "require");
            for (String ns : NAMESPACES) {
                require.invoke(Clojure.read(ns));
            }
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
