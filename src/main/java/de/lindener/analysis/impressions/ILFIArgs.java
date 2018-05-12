package de.lindener.analysis.impressions;

import com.beust.jcommander.Parameter;

public class ILFIArgs {
    @Parameter(names = {"--bound", "-b"})
    public int bound = 100000000;
    @Parameter(names = {"--cookies", "-c"})
    public int cookies = 10000;
    @Parameter(names = {"--websites", "-w"})
    public int websites = 1000000;
    @Parameter(names = {"--emit-min", "-e"})
    public int emitMin = 0;
    @Parameter(names = {"--top", "-n"})
    public int top = 100;
    @Parameter(names = {"--map-size", "-m"})
    public int mapSize = 4096;

    @Override
    public String toString() {
        return "ILFIArgs{" +
                "bound=" + bound +
                ", cookies=" + cookies +
                ", websites=" + websites +
                ", emitMin=" + emitMin +
                ", top=" + top +
                '}';
    }
}
