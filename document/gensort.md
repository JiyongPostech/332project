# Gensort

-a        Generate ascii records required for PennySort or JouleSort.
          These records are also an alternative input for the other
          sort benchmarks.  Without this flag, binary records will be
          generated that contain the highest density of randomness in
          the 10-byte key.

-bN       Set the beginning record generated to N. By default the
          first record generated is record 0.

### Example
ASCII
```
gensort -a -b      10000 p1
gensort -a -b10000 10000 p2
gensort -a -b20000 10000 p3
...