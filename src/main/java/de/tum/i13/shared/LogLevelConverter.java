package de.tum.i13.shared;

import picocli.CommandLine;

import java.util.logging.Level;

public class LogLevelConverter implements CommandLine.ITypeConverter<Level> {
    @Override
    public Level convert(String s) throws Exception {
        return Level.parse(s);
    }
}
