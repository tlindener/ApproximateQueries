package de.lindener.analysis.wikitrace;

import com.beust.jcommander.Parameter;

public class WTFIArgs {
    @Parameter(names = {"--emit-min", "-e"})
    public int emitMin = 0;
    @Parameter(names = {"--bound", "-b"})
    public int bound = 0;

    @Override
    public String toString() {
        return "AZFIArgs{" +
                "emitMin=" + emitMin +
                ", bound=" + bound +
                ", mapSize=" + mapSize +
                '}';
    }

    @Parameter(names = {"--map-size", "-m"})
    public int mapSize = 4096;

}
