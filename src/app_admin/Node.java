package app_admin;

import adminclient.AdminCommInterface;
import adminclient.AdminStore;
import common.HashRange;

public class Node {

    private String name;
    private String address;
    private String port;
    private AdminCommInterface store;
    private String md5Hash;

    public Node(String name, String address, String port) {
        this.name = name;
        this.address = address;
        this.port = port;
        this.md5Hash = HashRange.getMd5Hash(String.format("%s:%s", address, port));
        this.store = new AdminStore(this.address, Integer.parseInt(this.port));
    }

    public String getName() {
        return name;
    }

    public String getKey() {
        return String.format("%s:%s", address, port);
    }

    public String getAddress() {
        return address;
    }

    public String getPort() {
        return port;
    }

    public String getMD5Key() {
        return md5Hash;
    }

    public AdminCommInterface getStore() {
        return store;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setStore(AdminCommInterface store) {
        this.store = store;
    }

}
