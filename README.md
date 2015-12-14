# seaflow_cluster

This repo contains a Hadoop implementation of Gaussian Mixture Models. It is used to analyze oceanic cytometry
data from the SeaFlow cytometer.

<b>Initialization:</b>

The inputs to this Hadoop program are the input data to be classified and the initial parameters.
The input data is a CSV with the following schema:

File name, particle number, Number of dimensions (4), Fsc Small, Fsc Perp, PE, Chl Small

The initial parameters are stored as a CSV of initial cluster points with the following schema:

Number of dimensions (4), cluster value in each dimension (4 values), variance matrix (4x4 values), cluster weight.

<b>Generating the initial data:</b>

GMM is sensitive to initialization, so it's important to try to get the best initial points as possible.
This is the method that I used:

Pick k, the number of clusters. From a subset of the data, run a k-means++ initilation (there are many implementations in
python and R of this algorithm which usually provide access to the initial points). k-means++ has an initialization procedure
that separates the initial points of the k-means algorithm and has been extremely useful in improving the accuracy of that
algorithm.

The values of the initial points from k-means++ will serve as the cluster values as described in the schema for
initial points. I initialized the variance matrices to be identity matrices and the cluster weight to be 1/k for each
cluster.

<b>Running the pipeline:</b>

Once the data is ingested into hdfs, the clustering pipeline is run with the following command (as an example):

hadoop jar jar-name emmapreduce.EMDriver -libjars Jama-1.0.3.jar input-path filter-path path-to-parameters output-path k iters

jar-name is the name of the compiled jar
input-path is the path to the data.
filter-path is a path to the input data after noisy points have been filtered based on their chlorophyll content.
path-to-parameters is the path to the initial parameters.
output-path is the path to a directory where outputs per iteration are stored.
k is the number of clusters
iters is the number of iterations to run.

Here is an example run:

hadoop jar gmm_general_j6.jar \
emmapreduce.EMDriver \
    -libjars Jama-1.0.3.jar \
    hdfs:///user/root/input/all_files.csv \
    hdfs:///user/root/input/all_files_filtered \
    hdfs:///user/root/input/k7_init.txt \
    hdfs:///user/root/output/ \
    7 100


