module.exports = function (config) {
    config.set({
        browsers: ['ChromeHeadless'],
        basePath: 'target',
        files: ['browser-ci.js'],
        frameworks: ['cljs-test'],
        plugins: ['karma-cljs-test', 'karma-chrome-launcher'],
        colors: true,
        logLevel: config.LOG_INFO,
        client: {
            args: ["shadow.test.karma.init"]
        },
        singleRun: true,
        // Integration tests need longer timeouts due to database ops and network calls
        browserDisconnectTimeout: 180000,  // 3 minutes
        browserNoActivityTimeout: 180000,  // 3 minutes  
        captureTimeout: 300000              // 5 minutes
    })
};
