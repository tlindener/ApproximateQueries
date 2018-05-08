package de.lindener.analysis.amazon;

import com.beust.jcommander.Parameter;

public class AZFIArgs {
    @Parameter(names = {"--emit-min", "-e"})
    public int emitMin = 0;
    @Parameter(names = {"--bound", "-b"})
    public int bound = 0;

    @Override
    public String toString() {
        return "AZFIArgs{" +
                "emitMin=" + emitMin +
                ", bound=" + bound +
                '}';
    }
}
