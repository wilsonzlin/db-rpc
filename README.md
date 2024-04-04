MariaDB connections are highly efficient. However, pooling on the client side still leaves too many idle hanging/lingering connections, because of bursts. A max pool size limit could help, but it's hard to know the best value (it requires knowing exact consistent performance characteristics) and leaves too much performance on the table or still results in lingering connections.

With a lot of workers (which we tend to have), even with just a handful of connections, it adds up to thousands of connections overall. This results in congestion: clients and sockets tend to timeout or fail to establish. This is made worse if most of the connections are idle.

With a centralised pool, worker requests interleave so connections are maximally/efficiently used and idle time is minimised. Bursts are lower and less frequent. An upper limit on the (global) pool size can represent a global optimum; on the other hand, limits at each worker represents a coordination problem, and one worker's idle connections can't be used by another, but constantly dropping and reestablishing isn't feasible either.

The HTTP/2 protocol is also much more lightweight and scalable than the MySQL protocol. With thousands of connections/requests per second, it's no problem for HTTP/2; connections can be started and stopped rapidly too.
