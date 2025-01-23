package com.fxm.local.collection.test;

import com.fxm.local.collection.LocalList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAdd {
    @Test
    public void test1() throws Exception {
        try (LocalList<String> list = new LocalList<>(String.class);) {
            list.add("a");
            list.add("b");
            assertEquals("a", list.get(0));
            assertEquals("b", list.get(1));
        }
    }
}
