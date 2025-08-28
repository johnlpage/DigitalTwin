package com.mongodb.examples.java.mongotwin;

// Simplest fastest RNG implementation

public class FastRCG {
    int seed = 0;

    public void setSeed(int seed) {
        this.seed = seed;
    }

    public int nextInt(int range) {
        seed = fastRandom(seed);
        return seed % range;
    }

    public int nextInt(int seed, int range) {
        this.seed = seed;
        this.seed = fastRandom(this.seed);
        return this.seed % range;
    }

    int fastRandom(int seed) {
        return (seed * 1103515245 + 12345) & 0x7fffffff;
    }

}
