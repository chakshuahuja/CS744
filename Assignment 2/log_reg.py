import os
import numpy as np
import tensorflow as tf



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




# predictions_check = tf.equal(tf.argmax(y_hat, 1), tf.argmax(y, 1))
# accuracy_f = tf.reduce_mean(tf.cast(predictions_check, tf.float32))
#
#
# n_batches = int(60000/batch_size)
#
# with tf.Session() as tfs:
#     tf.global_variables_initializer().run()
#
# for epoch in range(n_epochs):
#     mnist.reset_index()
#
#     for batch in range(n_batches):
#         x_batch, y_batch = mnist.next_batch()
#         feed_dict={x: x_batch, y: y_batch}
#         batch_loss,_ = tfs.run([loss_f, optimizer_f],feed_dict=feed_dict )
#         print('Batch loss:{}'.format(batch_loss))