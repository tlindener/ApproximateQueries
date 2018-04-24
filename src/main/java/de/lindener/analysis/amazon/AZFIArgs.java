package de.lindener.analysis.amazon;

import com.beust.jcommander.Parameter;

public class AZFIArgs {
    @Parameter(names = {"--emit-min", "-e"})
    public int emitMin = 1000;
    @Parameter(names = {"--top", "-n"})
    public int top = 100;

    @Override
    public String toString() {
        return "ILFIArgs{" +
                ", emitMin=" + emitMin +
                ", top=" + top +
                '}';
    }
}
