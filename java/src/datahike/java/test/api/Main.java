package datahike.java.test.api;

import datahike.java.api.Api;

public class Main {
    public static void main(String[] args) {
        String uri = "datahike:mem://test-empty-db";
        Api.createDatabase(uri);
        Api.connect(uri);
    }
}
