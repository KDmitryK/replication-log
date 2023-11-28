package com.ds.replicationlog.master.controller;

public record Message(String data, int replicationFactor) {
}
