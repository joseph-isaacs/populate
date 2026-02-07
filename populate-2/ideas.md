## Ideas

I think we also want to support default_sink(cap) for a distribution which will
alloc a sink with specific bufferes etc all the way down.

Support skip n values into a distribution to allow parallel sink generation --
can have n threads jump i/N for i in 0..n and each generate small amount in parallel.
how to we know when we have done this is full walk of the graph, is there points that can skip
easy, where do we track state?