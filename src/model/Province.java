package model;

public class Province {
    private String name;
    private String url;

    public Province(String name, String url) {
        this.name = name;
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public String toString() {
        return "Province:" + this.name + "\t" + "url:" + this.url;
    }
}
