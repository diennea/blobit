<p class="intro">
BlobIt is a binary large object distributed storage implemented in Java. It has been designed to be embeddable in any Java Virtual Machine. It has been designed for very low latency and it does not rely on any shared media.
</p>

<h2>Scalability</h2>
BlobIt leverage Apache BookKeeper decentralized design and so it can scale automatically. It is simple to add and remove hosts to easly redistribute the load on multiple systems without manual configurations.

<h2>Resiliency</h2>
BlobIt leverages <a href="http://zookeeper.apache.org/" >Apache Zookeeper </a> and <a href="http://bookkeeper.apache.org/" >Apache Bookkeeper </a>to build a fully replicated, shared-nothing architecture without any single point of failure.

