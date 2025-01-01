package io.owen.audio;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class GuitarScale {
    private static final Map<String, String> noteMap = new HashMap<>();

    static {
        // Map notes to file paths
        noteMap.put("A", "kafka/samples/Guitar_A2.wav");
        noteMap.put("A#", "kafka/samples/Guitar_A#2.wav");
        noteMap.put("B", "kafka/samples/Guitar_B2.wav");
        noteMap.put("C", "kafka/samples/Guitar_C2.wav");
        noteMap.put("C#", "kafka/samples/Guitar_C#2.wav");
        noteMap.put("D", "kafka/samples/Guitar_D2.wav");
        noteMap.put("D#", "kafka/samples/Guitar_D#2.wav");
        noteMap.put("E", "kafka/samples/Guitar_E2.wav");
        noteMap.put("K", "kafka/samples/Guitar_E3.wav");
        noteMap.put("F", "kafka/samples/Guitar_F2.wav");
        noteMap.put("F#", "kafka/samples/Guitar_F#2.wav");
        noteMap.put("G", "kafka/samples/Guitar_G2.wav");
        noteMap.put("G#", "kafka/samples/Guitar_G#2.wav");
    }

    public static File getAudioFile(String note) {
        String filePath = noteMap.get(note.toUpperCase());
        if (filePath == null) {
            throw new IllegalArgumentException("Note " + note + " is not recognized.");
        }
        return new File(filePath);
    }
}
