Recommander
===========

Map-Reduce implementation of a recommander system using a content based approach. 

## Introduction

The goal of this project is to implement a recommendation system for the Netflix prize challenge. The requirement was to implement collaborative filtering using Hadoop (an open-source MapReduce [2] implementation). The Netflix Prize was an open competition for implementing the best collaborative filtering
algorithm for recommending movies to subscribed users. The grand prize of one million dollars was given to the winning team. For more information about the Netflix prize see
http://en.wikipedia.org/wiki/Netflix_Prize.


## Description

We implement a recommender system using the UV-decomposition algorithm as described in "Mining of Massive Datasets" textbook, chapter 9 [1]. The implementation has to be done using Hadoop to make use of distributed computation.

UV decomposition is an iterative algorithm, so we designed and implemented the Map and Reduce phases of each iteration that run in cascade. We maintain the connection between these iterations by taking the output of iteration i as the input of iteration i+1. Use the RMSE method to estimate the error of our solution. It also serve as a stopping criterion between iterations; We stop our iterative process if the error difference between two iterations falls below 0.01 or if the algorithm runs more than 20 minutes (or ~10 iterations). You can edit that in the Main.java source code.

### Input

We are given an input dataset that resides on HDFS on which we will perform this UV-decomposition. The data contains about 98 million ratings that 480189 users gave to 17770 movies. The format of this file is as follows:

\<userID, movieID, grade, date-of-grade\>

where grade has integer values from 1-5. UserID and MovieID have integer values that range from 1 to 480189 and 17770, respectively. Note that while the actual grades are integers in the range 1 to 5, submitted predictions need not be. This dataset having a massive size that is not suitable for github, we provide a smaller dataset that contains ratings of 5000 users for 100 movies (/std11/inputs/). The dataset can be be conveniently run on your local machine. Before running your code on the cluster, perform experiments on your local machine
exhaustively.

### Output

Our output are the U and V matrices. The output format of these matrices is as follows

"<U, userID, [1..10], value>" and
"<V, [1..10], movieID, value>"

The output matrices are stored as one or more text files on HDFS according to the following rules:
1) We store U and V of one iteration in two separate directories, we are required to use the following path formats:
/std11/output/U_i
/std11/output/V_i
where U_i is a directory that contains matrix U at iteration i.
The produced matrices approximate the normalized utility matrix; that is, we do not undo the normalization step.

#Disclaimer
Don’t expect great response times. Hadoop is always a bit sluggish – even if the system is not heavily loaded, it is not strikingly efficient, which is annoying for small and simple jobs, but it is scalable. Don't be frustrated about the Hadoop performance, it's not necessarily a problem in the code.


#References
[1] Mining of Massive Datasets -Anand Rajaraman (Kosmix, Inc). Jeffrey D. Ullman (Stanford Univ).
[2] Google

#Note on our Runtime Environment

Our cluster has four blades and it runs a virtualized Solaris-based environment with 96 hardware threads (~=cores). Four nodes are designated gateways dedicated for communication with the outside world (called “global zones” on Solaris), i.e. they are visible globally and can be connected to via ssh. These nodes are called **** (edited). Each global zone manages a blade and shares memory and I/O with 22 "local zones". These
are virtual machines that each have a hardware thread exclusively assigned to them -- so work
can run on each of these local zones in parallel. The names of these zones have the following
format: ******* (edited). Each local zone has 2GB of RAM assigned to it. Hadoop version 0.21 has been installed on ***** as follows. The system is configured with 88 worker nodes, with the nameserver and jobtracker running on *****. Status information about namenode and jobtracker can be found at:
http://******/(namenode)
http://******/(jobtracker)
Data is stored on HDFS, Hadoop's distributed and replicated file system.

A tutorial for writing MapReduce programs in Hadoop can be found on:
http://hadoop.apache.org/docs/r1.1.1/mapred_tutorial.html

It is possible (and arguably not too difficult) to create a small Hadoop installation on your own computer/laptop for early-stage testing. Please follow the instructions for “Standalone operation” on 
http://hadoop.apache.org/docs/r1.1.2/single_node_setup.html
