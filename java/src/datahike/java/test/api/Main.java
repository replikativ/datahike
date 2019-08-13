package datahike.java.test.api;

import datahike.java.api.Api;

public class Main {
    public static void main(String[] args) {
        Api.connect("datahike:mem://test-empty-db");
    }
}
