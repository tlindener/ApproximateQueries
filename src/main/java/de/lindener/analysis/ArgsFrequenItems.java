package de.lindener.analysis;

import com.beust.jcommander.Parameter;

public class ArgsFrequenItems {
    @Parameter(names = {"--bound", "-b"})
    int bound = 10000;
    @Parameter(names = {"--cookies", "-c"})
    int cookies = 1000;
    @Parameter(names = {"--websites", "-w"})
    int websites = 1000;
    @Parameter(names = {"--emit-min", "-e"})
    int emitMin = 100;
    @Parameter(names = {"--top", "-n"})
    int top = 10;
}
