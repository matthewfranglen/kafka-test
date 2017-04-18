package com.matthew.dto;

public class Message {

    public static final Message MESSAGE = new Message("The Magic Words are Squeamish Ossifrage");

    private String message;

    public Message(String message) {
        this.message = message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

}
