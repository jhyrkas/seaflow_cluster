# seaflow_cluster

This repo contains a Hadoop implementation of Gaussian Mixture Models. It is used to analyze oceanic cytometry
data from the SeaFlow cytometer.

Initialization:

The inputs to this Hadoop program are the input data to be classified and the initial parameters.
The input data is a CSV with the following schema:

File name, particle number, Number of dimensions (4), Fsc Small, Fsc Perp, PE, Chl Small

The initial parameters are stored as a CSV of initial cluster points with the following schema:

Number of dimensions (4), mean per column (4 values), variance matrix (4x4 values).

Generating the initial data:




Running the pipeline:

Once the data is ingested into hdfs, the clustering pipeline is run with the following command (as an example):

emmapreduce.EMDriver -libjars Jama-1.0.3.jar input-path filter-path path-to-parameters output-path k iters

input-path is the path to the data.
filter-path is a path to the input data after noisy points have been filtered based on their chlorophyll content.
path-to-parameters is the path to the initial parameters.
output-path is the path to a directory where outputs per iteration are stored.
k is the number of clusters
iters is the number of iterations to run.

Here is an example run:

emmapreduce.EMDriver \
    -libjars Jama-1.0.3.jar \
    hdfs:///user/root/input/all_files.csv \
    hdfs:///user/root/input/all_files_filtered \
    hdfs:///user/root/input/k7_init.txt \
    hdfs:///user/root/output/ \
    7 100


