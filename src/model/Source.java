package model;

public class Source {
    private int id;
    private String name;
    private String url;

    public Source(int id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }

    @Override
    public String toString() {
        return "Source:" + this.name + "\t" + "url:" + this.url;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }
}
