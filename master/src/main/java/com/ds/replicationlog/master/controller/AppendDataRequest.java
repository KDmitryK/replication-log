package com.ds.replicationlog.master.controller;

public record AppendDataRequest(String data, int minAcknowledgments) {
}
