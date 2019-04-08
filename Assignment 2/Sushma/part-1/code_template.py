import tensorflow as tf
import os
import numpy as np

# define the command line flags that can be sent
tf.app.flags.DEFINE_integer("task_index", 0, "Index of task with in the job.")
tf.app.flags.DEFINE_string("job_name", "worker", "either worker or ps")
tf.app.flags.DEFINE_string("deploy_mode", "single", "either single or cluster")
FLAGS = tf.app.flags.FLAGS

tf.logging.set_verbosity(tf.logging.DEBUG)

clusterSpec_single = tf.train.ClusterSpec({
    "worker" : [
        "localhost:2232"
    ]
})

clusterSpec_cluster = tf.train.ClusterSpec({
    "ps" : [
        "c220g5-110421:2232"
    ],
    "worker" : [
        "c220g5-110423:2232",
        "c220g5-110429:2232"
    ]
})

clusterSpec_cluster2 = tf.train.ClusterSpec({
    "ps" : [
        "host_name0:2232"
    ],
    "worker" : [
        "c220g5-110421:2233",
        "c220g5-110423:2232",
        "c220g5-110429:2232",
    ]
})

clusterSpec = {
    "single": clusterSpec_single,
    "cluster": clusterSpec_cluster,
    "cluster2": clusterSpec_cluster2
}

clusterinfo = clusterSpec[FLAGS.deploy_mode]
server = tf.train.Server(clusterinfo, job_name=FLAGS.job_name, task_index=FLAGS.task_index)

if FLAGS.job_name == "ps":
    server.join()
elif FLAGS.job_name == "worker":
	from tensorflow.examples.tutorials.mnist import input_data
	mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

	learning_rate = 0.001
	n_epochs = 5
	batch_size = 100
	n_features = 784
	n_classes = 10

	x = tf.placeholder(dtype=tf.float32, shape=[None, n_features])
	y = tf.placeholder(dtype=tf.float32, shape=[None, n_classes])
	w = tf.Variable(tf.zeros([n_features, n_classes]))
	b = tf.Variable(tf.zeros([n_classes]))

	prediction = tf.nn.softmax(tf.matmul(x, w) + b)
	loss = tf.reduce_mean(-tf.reduce_sum(y*tf.log(prediction), reduction_indices=1))

	optimizer = tf.train.GradientDescentOptimizer
	optimizer_f = optimizer(learning_rate=learning_rate).minimize(loss)


	init = tf.initialize_all_variables()
	sess = tf.Session()
	sess.run(init)

	for i in range(1000):
	  batch_xs, batch_ys = mnist.train.next_batch(100)
	  sess.run(optimizer_f, feed_dict={x: batch_xs, y: batch_ys})
	  correct_prediction = tf.equal(tf.argmax(prediction, 1), tf.argmax(y, 1))
	  accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))
	  if i%100 == 0:
	    print(sess.run(accuracy, feed_dict={x: mnist.test.images, y: mnist.test.labels}))


